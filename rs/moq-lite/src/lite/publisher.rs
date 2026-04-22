use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use web_async::FuturesExt;
use web_transport_trait::Stats;

use crate::{
	AsPath, BroadcastConsumer, Error, Origin, OriginConsumer, Track, TrackConsumer,
	coding::{Stream, Writer},
	lite::{
		self,
		priority::{PriorityHandle, PriorityQueue},
	},
	model::GroupConsumer,
};

use super::Version;
use super::shed::ShedController;

pub(super) struct Publisher<S: web_transport_trait::Session> {
	session: S,
	origin: OriginConsumer,
	priority: PriorityQueue,
	version: Version,
	shed: Option<Arc<ShedController>>,
}

impl<S: web_transport_trait::Session> Publisher<S> {
	pub fn new(session: S, origin: Option<OriginConsumer>, version: Version) -> Self {
		// Default to a dummy origin that is immediately closed.
		let origin = origin.unwrap_or_else(|| Origin::produce().consume());
		Self {
			session,
			origin,
			priority: Default::default(),
			version,
			shed: None,
		}
	}

	/// Enable relay-side bandwidth shedding for this publisher.
	///
	/// When enabled, each subscriber connection gets a ShedController that monitors
	/// the QUIC CC bandwidth estimate and sheds low-priority tracks when bandwidth
	/// is scarce. This should only be called by the relay code path.
	pub fn with_shedding(mut self) -> Self {
		self.shed = Some(Arc::new(ShedController::new(std::time::Duration::ZERO)));
		self
	}

	pub async fn run(mut self) -> Result<(), Error> {
		loop {
			let mut stream = Stream::accept(&self.session, self.version).await?;

			// To avoid cloning the origin, we process each control stream in received order.
			// This adds some head-of-line blocking but it delays an expensive clone.
			let kind = stream.reader.decode().await?;

			if let Err(err) = match kind {
				lite::ControlType::Announce => self.recv_announce(stream).await,
				lite::ControlType::Subscribe => self.recv_subscribe(stream).await,
				lite::ControlType::Probe => {
					self.recv_probe(stream);
					Ok(())
				}
				lite::ControlType::Goaway => {
					tracing::info!("received goaway stream");
					Ok(())
				}
				lite::ControlType::Session | lite::ControlType::Fetch => Err(Error::UnexpectedStream),
			} {
				tracing::warn!(%err, "control stream error");
			}
		}
	}

	fn recv_probe(&self, mut stream: Stream<S, Version>) {
		let session = self.session.clone();
		let version = self.version;

		web_async::spawn(async move {
			if let Err(err) = Self::run_probe(&session, &mut stream, version).await {
				match &err {
					Error::Cancel | Error::Transport(_) => {
						tracing::debug!("probe stream closed");
					}
					err => {
						tracing::warn!(%err, "probe stream error");
					}
				}
				stream.writer.abort(&err);
			} else {
				tracing::debug!("probe stream complete");
			}
		});
	}

	async fn run_probe(session: &S, stream: &mut Stream<S, Version>, _version: Version) -> Result<(), Error> {
		const PROBE_INTERVAL: Duration = Duration::from_millis(100);
		const PROBE_MAX_AGE: Duration = Duration::from_secs(10);
		const PROBE_MAX_DELTA: f64 = 0.25;

		let mut last_sent: Option<(u64, tokio::time::Instant)> = None;
		let mut interval = tokio::time::interval(PROBE_INTERVAL);

		loop {
			tokio::select! {
				res = stream.reader.closed() => return res,
				_ = interval.tick() => {}
			}

			let Some(bitrate) = session.stats().estimated_send_rate() else {
				continue;
			};

			let should_send = match last_sent {
				None => true,
				Some((0, _)) => bitrate > 0,
				Some((prev, at)) => {
					let elapsed = at.elapsed().as_secs_f64();
					let t = elapsed.clamp(PROBE_INTERVAL.as_secs_f64(), PROBE_MAX_AGE.as_secs_f64());
					let range = PROBE_MAX_AGE.as_secs_f64() - PROBE_INTERVAL.as_secs_f64();
					let threshold = PROBE_MAX_DELTA * (PROBE_MAX_AGE.as_secs_f64() - t) / range;
					let change = (bitrate as f64 - prev as f64).abs() / prev as f64;
					change >= threshold
				}
			};

			if should_send {
				let rtt = session.stats().rtt().map(|d| d.as_millis() as u64);
				stream.writer.encode(&lite::Probe { bitrate, rtt }).await?;
				last_sent = Some((bitrate, tokio::time::Instant::now()));
			}
		}
	}

	pub async fn recv_announce(&mut self, mut stream: Stream<S, Version>) -> Result<(), Error> {
		let interest = stream.reader.decode::<lite::AnnounceInterest>().await?;
		let prefix = interest.prefix.to_owned();

		let mut origin = self
			.origin
			.consume_only(&[prefix.as_path()])
			.ok_or(Error::Unauthorized)?;

		let version = self.version;
		web_async::spawn(async move {
			if let Err(err) = Self::run_announce(&mut stream, &mut origin, &prefix, version).await {
				match &err {
					Error::Cancel | Error::Transport(_) => {
						tracing::debug!(prefix = %origin.absolute(prefix), "announcing cancelled");
					}
					err => {
						tracing::warn!(%err, prefix = %origin.absolute(prefix), "announcing error");
					}
				}

				stream.writer.abort(&err);
			}
		});

		Ok(())
	}

	async fn run_announce(
		stream: &mut Stream<S, Version>,
		origin: &mut OriginConsumer,
		prefix: impl AsPath,
		version: Version,
	) -> Result<(), Error> {
		let prefix = prefix.as_path();

		match version {
			Version::Lite01 | Version::Lite02 => {
				let mut init = Vec::new();

				// Send ANNOUNCE_INIT as the first message with all currently active paths
				// We use `try_next()` to synchronously get the initial updates.
				while let Some((path, active)) = origin.try_announced() {
					let suffix = path.strip_prefix(&prefix).expect("origin returned invalid path");

					if active.is_some() {
						tracing::debug!(broadcast = %origin.absolute(&path), "announce");
						init.push(suffix.to_owned());
					} else {
						// A potential race.
						tracing::debug!(broadcast = %origin.absolute(&path), "unannounce");
						init.retain(|path| path != &suffix);
					}
				}

				let announce_init = lite::AnnounceInit { suffixes: init };
				stream.writer.encode(&announce_init).await?;
			}
			_ => {
				// Lite03+: no more announce init.
			}
		}

		// Send updates as they arrive.
		loop {
			tokio::select! {
				biased;
				res = stream.reader.closed() => return res,
				announced = origin.announced() => {
					match announced {
						Some((path, active)) => {
							let suffix = path.strip_prefix(&prefix).expect("origin returned invalid path").to_owned();

							if active.is_some() {
								tracing::debug!(broadcast = %origin.absolute(&path), "announce");
								let msg = lite::Announce::Active { suffix, hops: Vec::new() };
								stream.writer.encode(&msg).await?;
							} else {
								tracing::debug!(broadcast = %origin.absolute(&path), "unannounce");
								let msg = lite::Announce::Ended { suffix, hops: Vec::new() };
								stream.writer.encode(&msg).await?;
							}
						},
						None => {
							stream.writer.finish()?;
							return stream.writer.closed().await;
						}
					}
				}
			}
		}
	}

	pub async fn recv_subscribe(&mut self, mut stream: Stream<S, Version>) -> Result<(), Error> {
		let subscribe = stream.reader.decode::<lite::Subscribe>().await?;

		let id = subscribe.id;
		let track = subscribe.track.clone();
		let absolute = self.origin.absolute(&subscribe.broadcast).to_owned();

		tracing::info!(%id, broadcast = %absolute, %track, "subscribed started");

		let broadcast = self.origin.consume_broadcast(&subscribe.broadcast);
		let priority = self.priority.clone();
		let version = self.version;
		let shed = self.shed.clone();

		let session = self.session.clone();
		web_async::spawn(async move {
			if let Err(err) = Self::run_subscribe(session, &mut stream, &subscribe, broadcast, priority, version, shed).await
			{
				match &err {
					// TODO better classify WebTransport errors.
					Error::Cancel | Error::Transport(_) => {
						tracing::info!(%id, broadcast = %absolute, %track, "subscribed cancelled")
					}
					err => {
						tracing::warn!(%id, broadcast = %absolute, %track, %err, "subscribed error")
					}
				}
				stream.writer.abort(&err);
			} else {
				tracing::info!(%id, broadcast = %absolute, %track, "subscribed complete")
			}
		});

		Ok(())
	}

	async fn run_subscribe(
		session: S,
		stream: &mut Stream<S, Version>,
		subscribe: &lite::Subscribe<'_>,
		consumer: Option<BroadcastConsumer>,
		priority: PriorityQueue,
		version: Version,
		shed: Option<Arc<ShedController>>,
	) -> Result<(), Error> {
		let track = Track {
			name: subscribe.track.to_string(),
			priority: subscribe.priority,
		};

		let broadcast = consumer.ok_or(Error::NotFound)?;
		let track = broadcast.subscribe_track(&track)?;

		// TODO wait until track.info() to get the *real* priority

		let info = lite::SubscribeOk {
			priority: track.info.priority,
			ordered: false,
			max_latency: std::time::Duration::ZERO,
			start_group: None,
			end_group: None,
		};

		stream.writer.encode(&lite::SubscribeResponse::Ok(info)).await?;

		let (drop_tx, mut drop_rx) = tokio::sync::mpsc::channel::<(u64, u64)>(16);
		let mut track_fut = std::pin::pin!(
			Self::run_track(session, track, subscribe, priority, version, shed, drop_tx)
		);

		loop {
			tokio::select! {
				res = &mut track_fut => { res?; break; },
				res = stream.reader.closed() => { res?; break; },
				Some((start, end)) = drop_rx.recv() => {
					let msg = lite::SubscribeResponse::Drop(lite::SubscribeDrop {
						start, end, error: 0,
					});
					match stream.writer.encode(&msg).await {
						Ok(()) => {
							tracing::debug!(
								id = %subscribe.id,
								start,
								end,
								"sent SubscribeDrop"
							);
						}
						Err(Error::Encode(crate::coding::EncodeError::Version)) => {
							// Lite01/02: no SubscribeDrop support, silently skip.
						}
						Err(e) => return Err(e),
					}
				},
			}
		}

		stream.writer.finish()?;
		stream.writer.closed().await
	}

	async fn run_track(
		session: S,
		mut track: TrackConsumer,
		subscribe: &lite::Subscribe<'_>,
		priority: PriorityQueue,
		version: Version,
		shed: Option<Arc<ShedController>>,
		drop_tx: tokio::sync::mpsc::Sender<(u64, u64)>,
	) -> Result<(), Error> {
		let mut tasks = FuturesUnordered::new();
		let mut shed_range: Option<(u64, u64)> = None;

		// Start the consumer at the specified sequence, otherwise start at the latest group.
		if let Some(start_group) = subscribe.start_group.or_else(|| track.latest()) {
			track.start_at(start_group);
		}

		loop {
			let group = tokio::select! {
				// Poll all active group futures; never matches but keeps them running.
				true = async {
					while tasks.next().await.is_some() {}
					false
				} => unreachable!(),
				Some(group) = track.next_group().transpose() => group,
				else => {
					// Flush any pending shed range before returning.
					if let Some((start, end)) = shed_range.take() {
						let _ = drop_tx.try_send((start, end));
					}
					return Ok(());
				},
			}?;

			// Relay shedding: poll CC estimate and check if this track should be forwarded.
			if let Some(ref shed) = shed {
				shed.update(session.stats().estimated_send_rate());
				if !shed.should_forward(track.info.priority) {
					let sequence = group.info.sequence;
					tracing::trace!(
						subscribe = %subscribe.id,
						track = %track.info.name,
						priority = track.info.priority,
						sequence,
						"group shed by relay"
					);
					match &mut shed_range {
						Some((_, end)) => *end = sequence,
						None => shed_range = Some((sequence, sequence)),
					}
					continue;
				}
			}

			// Flush any pending shed range — a non-shed group breaks the run.
			if let Some((start, end)) = shed_range.take() {
				let _ = drop_tx.try_send((start, end));
			}

			let sequence = group.info.sequence;
			tracing::debug!(subscribe = %subscribe.id, track = %track.info.name, sequence, "serving group");

			let msg = lite::Group {
				subscribe: subscribe.id,
				sequence,
			};

			let priority = priority.insert(track.info.priority, sequence);
			tasks.push(Self::serve_group(session.clone(), msg, priority, group, version, shed.clone()).map(|_| ()));
		}
	}

	async fn serve_group(
		session: S,
		msg: lite::Group,
		mut priority: PriorityHandle,
		mut group: GroupConsumer,
		version: Version,
		shed: Option<Arc<ShedController>>,
	) -> Result<(), Error> {
		// TODO add a way to open in priority order.
		let stream = session.open_uni().await.map_err(Error::from_transport)?;

		let mut stream = Writer::new(stream, version);
		stream.set_priority(priority.current());
		stream.encode(&lite::DataType::Group).await?;
		stream.encode(&msg).await?;

		// Extract priority value once for byte reporting.
		let priority_value = priority.current();

		loop {
			let frame = tokio::select! {
				biased;
				_ = stream.closed() => return Err(Error::Cancel),
				frame = group.next_frame() => frame,
				// Update the priority if it changes.
				priority = priority.next() => {
					stream.set_priority(priority);
					continue;
				}
			};

			let mut frame = match frame? {
				Some(frame) => frame,
				None => break,
			};

			stream.encode(&frame.info.size).await?;

			loop {
				let chunk = tokio::select! {
					biased;
					_ = stream.closed() => return Err(Error::Cancel),
					chunk = frame.read_chunk() => chunk,
					// Update the priority if it changes.
					priority = priority.next() => {
						stream.set_priority(priority);
						continue;
					}
				};

				match chunk? {
					Some(mut chunk) => {
						let len = chunk.remaining();
						stream.write_all(&mut chunk).await?;
						if let Some(ref shed) = shed {
							shed.report_bytes(priority_value, len);
						}
					}
					None => break,
				}
			}
		}

		stream.finish()?;
		stream.closed().await?;

		tracing::debug!(sequence = %msg.sequence, "finished group");

		Ok(())
	}
}

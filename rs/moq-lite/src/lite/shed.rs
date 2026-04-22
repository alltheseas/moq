use std::{
	collections::{HashMap, VecDeque},
	sync::{atomic::{AtomicU8, Ordering}, Mutex},
	time::{Duration, Instant},
};

const RESTORE_HEADROOM: f64 = 1.5;
const MIN_SHED_DURATION: Duration = Duration::from_secs(5);
const BW_WINDOW: Duration = Duration::from_secs(5);
/// Minimum time between consecutive restore steps to prevent cascading restores.
const RESTORE_COOLDOWN: Duration = Duration::from_secs(2);

/// How recently a track must have received data to be considered "active."
/// Tracks with no groups in this window are treated as absent (publisher already shed them).
const ACTIVE_THRESHOLD: Duration = Duration::from_secs(2);

/// Sliding-minimum bandwidth filter.
///
/// Records (timestamp, bw) samples and returns the minimum over the last BW_WINDOW.
/// The minimum is the correct signal because CC estimates inflate when tracks are shed
/// (less data → more spare capacity → CC overestimates). The sliding-min captures
/// the true bottleneck, not transient spare capacity.
struct BwFilter {
	samples: VecDeque<(Instant, u64)>,
}

impl BwFilter {
	fn new() -> Self {
		Self {
			samples: VecDeque::new(),
		}
	}

	fn update(&mut self, bw: u64) -> u64 {
		let now = Instant::now();
		self.samples.push_back((now, bw));
		while let Some(&(t, _)) = self.samples.front() {
			if now.duration_since(t) > BW_WINDOW {
				self.samples.pop_front();
			} else {
				break;
			}
		}
		self.samples.iter().map(|&(_, v)| v).min().unwrap_or(bw)
	}
}

/// Rolling byte counter for a single priority level.
struct ThroughputBucket {
	bytes: u64,
	window_start: Instant,
	/// Bytes per second, computed at each window rollover.
	bps: u64,
	/// Last time any bytes were reported (for active/absent detection).
	last_seen: Instant,
	/// Cached bps at the moment the track was shed, for restore estimation.
	cached_bps: u64,
}

impl ThroughputBucket {
	fn new() -> Self {
		let now = Instant::now();
		Self {
			bytes: 0,
			window_start: now,
			bps: 0,
			last_seen: now,
			cached_bps: 0,
		}
	}

	fn report(&mut self, bytes: usize) {
		let now = Instant::now();
		self.last_seen = now;
		self.bytes += bytes as u64;

		let elapsed = now.duration_since(self.window_start);
		if elapsed >= Duration::from_secs(1) {
			self.bps = (self.bytes as f64 / elapsed.as_secs_f64()) as u64 * 8;
			self.bytes = 0;
			self.window_start = now;
		} else if elapsed >= Duration::from_millis(100) && self.bps == 0 {
			// Compute interim bps before the first full window completes.
			// Without this, observed_total stays at 0 for the first second,
			// preventing shedding during the critical initial burst.
			self.bps = (self.bytes as f64 / elapsed.as_secs_f64()) as u64 * 8;
		}
	}

	/// Returns true if this priority has had active data in the last ACTIVE_THRESHOLD.
	fn is_active(&self) -> bool {
		self.last_seen.elapsed() < ACTIVE_THRESHOLD
	}
}

struct ShedInner {
	bw_filter: BwFilter,
	throughput: HashMap<u8, ThroughputBucket>,
	/// Priority → Instant when shedding started for this priority.
	shed_since: HashMap<u8, Instant>,
	/// Last time a restore step was executed (cooldown between steps).
	last_restore: Option<Instant>,
}

/// Per-subscriber bandwidth-aware shed controller.
///
/// Shared (via `Arc`) across all `run_track` tasks for a single subscriber.
/// Observes per-priority throughput and the QUIC CC bandwidth estimate,
/// then adjusts a priority cutoff: tracks with `priority < cutoff` are shed.
///
/// Priority 255 is never shed (safety/heartbeat).
pub(super) struct ShedController {
	inner: Mutex<ShedInner>,
	/// Tracks with priority < this value are shed.
	priority_cutoff: AtomicU8,
	started_at: Instant,
	grace_period: Duration,
}

impl ShedController {
	/// Create with a grace period. Use `Duration::ZERO` for relay shedding
	/// (CC estimate is already valid), `Duration::from_secs(5)` for publisher shedding
	/// (CC starts from zero).
	pub fn new(grace_period: Duration) -> Self {
		Self {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now(),
			grace_period,
		}
	}

	/// Returns true if a track at the given priority should be forwarded right now.
	pub fn should_forward(&self, priority: u8) -> bool {
		// Grace period: don't shed during CC ramp-up after session start.
		if self.started_at.elapsed() < self.grace_period {
			return true;
		}
		priority >= self.priority_cutoff.load(Ordering::Relaxed)
	}

	/// Report bytes written for a track at the given priority.
	pub fn report_bytes(&self, priority: u8, bytes: usize) {
		let mut inner = self.inner.lock().unwrap();
		inner
			.throughput
			.entry(priority)
			.or_insert_with(ThroughputBucket::new)
			.report(bytes);
	}

	/// Poll CC estimate and update the priority cutoff.
	///
	/// Called from `run_track` on each group arrival. The caller provides the
	/// raw CC estimate from `session.stats().estimated_send_rate()`.
	pub fn update(&self, raw_bw: Option<u64>) {
		// Grace period: no shedding decisions.
		if self.started_at.elapsed() < self.grace_period {
			return;
		}

		let Some(raw_bw) = raw_bw else {
			return;
		};

		let mut inner = self.inner.lock().unwrap();
		let filtered_bw = inner.bw_filter.update(raw_bw);

		// Compute observed throughput from active tracks only.
		// Inactive tracks (publisher already shed them) are excluded to avoid
		// miscounting — we can't shed what isn't being sent.
		let mut priorities: Vec<(u8, u64)> = inner
			.throughput
			.iter()
			.filter(|(_, bucket)| bucket.is_active())
			.map(|(&p, bucket)| (p, bucket.bps))
			.collect();

		// Sort ascending by priority (lowest priority = most expendable = shed first).
		priorities.sort_by_key(|&(p, _)| p);

		let observed_total: u64 = priorities.iter().map(|&(_, bps)| bps).sum();
		let current_cutoff = self.priority_cutoff.load(Ordering::Relaxed);

		if filtered_bw < observed_total {
			// Sanity check: if the raw (current) CC estimate shows ample bandwidth,
			// the filtered sliding-min is likely a stale artifact from CC ramp-up,
			// not real congestion. Skip shedding.
			if raw_bw > observed_total * 2 {
				return;
			}
			// Need to shed. Raise cutoff until remaining throughput fits within
			// 90% of filtered bandwidth (leave 10% headroom).
			let target = (filtered_bw as f64 * 0.9) as u64;
			let mut remaining = observed_total;
			let mut new_cutoff = current_cutoff;

			for &(priority, bps) in &priorities {
				if remaining <= target {
					break;
				}
				if priority >= 255 {
					continue; // Never shed P255
				}
				if priority < new_cutoff {
					continue; // Already shed
				}
				// Shed this priority level.
				remaining = remaining.saturating_sub(bps);
				new_cutoff = priority + 1;

				// Cache the bps at shed time for restore estimation (P4).
				if let Some(bucket) = inner.throughput.get_mut(&priority) {
					bucket.cached_bps = bps;
				}
				inner.shed_since.entry(priority).or_insert_with(Instant::now);
			}

			if new_cutoff != current_cutoff {
				tracing::info!(
					old_cutoff = current_cutoff,
					new_cutoff,
					filtered_bw_kbps = filtered_bw / 1000,
					raw_bw_kbps = raw_bw / 1000,
					observed_kbps = observed_total / 1000,
					"relay shed: raising cutoff"
				);
				self.priority_cutoff.store(new_cutoff, Ordering::Relaxed);
			}
		} else if current_cutoff > 0 {
			// Cooldown: don't restore more than one level per RESTORE_COOLDOWN.
			// Without this, update() is called on every group arrival (hundreds/sec),
			// causing cutoff to cascade from 255→0 in milliseconds → oscillation.
			let cooldown_ok = inner
				.last_restore
				.map(|t| t.elapsed() >= RESTORE_COOLDOWN)
				.unwrap_or(true);

			if !cooldown_ok {
				return;
			}

			// Consider restoring. Use cached bps for shed tracks (P4).
			// Compute what total would be if we restored the next priority level below cutoff.
			let restore_candidate = current_cutoff - 1;

			// Find the cached bps for the restore candidate.
			let candidate_bps = inner
				.throughput
				.get(&restore_candidate)
				.map(|b| b.cached_bps)
				.unwrap_or(0);

			let projected_total = observed_total + candidate_bps;
			let restore_threshold = (projected_total as f64 * RESTORE_HEADROOM) as u64;

			// Only restore if: bw has enough headroom AND the priority has been shed long enough.
			let held_long_enough = inner
				.shed_since
				.get(&restore_candidate)
				.map(|t| t.elapsed() >= MIN_SHED_DURATION)
				.unwrap_or(true);

			if filtered_bw > restore_threshold && held_long_enough {
				tracing::info!(
					old_cutoff = current_cutoff,
					new_cutoff = restore_candidate,
					filtered_bw_kbps = filtered_bw / 1000,
					raw_bw_kbps = raw_bw / 1000,
					observed_kbps = observed_total / 1000,
					projected_kbps = projected_total / 1000,
					"relay shed: lowering cutoff (restoring)"
				);
				self.priority_cutoff.store(restore_candidate, Ordering::Relaxed);
				inner.shed_since.remove(&restore_candidate);
				inner.last_restore = Some(Instant::now());
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn grace_period_always_forwards() {
		let ctrl = ShedController::new(Duration::from_secs(5));
		// During grace period, everything forwards even with cutoff set high.
		ctrl.priority_cutoff.store(100, Ordering::Relaxed);
		assert!(ctrl.should_forward(1));
		assert!(ctrl.should_forward(50));
		assert!(ctrl.should_forward(255));
	}

	#[test]
	fn cutoff_behavior() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			// Backdate started_at so grace period is over.
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		// Cutoff 0: everything forwards.
		assert!(ctrl.should_forward(0));
		assert!(ctrl.should_forward(1));
		assert!(ctrl.should_forward(255));

		// Cutoff 10: priority < 10 is shed.
		ctrl.priority_cutoff.store(10, Ordering::Relaxed);
		assert!(!ctrl.should_forward(0));
		assert!(!ctrl.should_forward(9));
		assert!(ctrl.should_forward(10));
		assert!(ctrl.should_forward(255));
	}

	#[test]
	fn p255_never_shed() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		// Even with max cutoff, P255 should forward.
		ctrl.priority_cutoff.store(255, Ordering::Relaxed);
		assert!(ctrl.should_forward(255));
		assert!(!ctrl.should_forward(254));
	}

	#[test]
	fn shed_raises_cutoff() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		// Simulate throughput: P1 = 30 Mbps, P150 = 5 Mbps, P255 = 100 kbps
		{
			let mut inner = ctrl.inner.lock().unwrap();
			let mut b1 = ThroughputBucket::new();
			b1.bps = 30_000_000;
			b1.last_seen = Instant::now();
			inner.throughput.insert(1, b1);

			let mut b150 = ThroughputBucket::new();
			b150.bps = 5_000_000;
			b150.last_seen = Instant::now();
			inner.throughput.insert(150, b150);

			let mut b255 = ThroughputBucket::new();
			b255.bps = 100_000;
			b255.last_seen = Instant::now();
			inner.throughput.insert(255, b255);
		}

		// CC estimate at 10 Mbps (< 35.1 Mbps total) → should shed P1.
		ctrl.update(Some(10_000_000));

		let cutoff = ctrl.priority_cutoff.load(Ordering::Relaxed);
		assert!(cutoff >= 2, "cutoff should be >= 2 to shed P1, got {cutoff}");
		assert!(ctrl.should_forward(150));
		assert!(ctrl.should_forward(255));
		assert!(!ctrl.should_forward(1));
	}

	#[test]
	fn restore_requires_headroom_and_hold() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(2), // P1 is shed
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		{
			let mut inner = ctrl.inner.lock().unwrap();
			// Active track at P150: 5 Mbps
			let mut b150 = ThroughputBucket::new();
			b150.bps = 5_000_000;
			b150.last_seen = Instant::now();
			inner.throughput.insert(150, b150);

			// Shed P1 with cached 30 Mbps, shed recently
			let mut b1 = ThroughputBucket::new();
			b1.cached_bps = 30_000_000;
			b1.last_seen = Instant::now() - Duration::from_secs(5); // inactive
			inner.throughput.insert(1, b1);

			inner.shed_since.insert(1, Instant::now()); // Just shed — too recent
		}

		// Plenty of bandwidth but too soon → should NOT restore.
		ctrl.update(Some(100_000_000));
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 2);

		// Backdate shed time.
		{
			let mut inner = ctrl.inner.lock().unwrap();
			inner.shed_since.insert(1, Instant::now() - Duration::from_secs(6));
		}

		// Now with enough time + enough bw → should restore.
		ctrl.update(Some(100_000_000));
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn absent_tracks_not_counted() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		{
			let mut inner = ctrl.inner.lock().unwrap();
			// Active track P255: 100 kbps
			let mut b255 = ThroughputBucket::new();
			b255.bps = 100_000;
			b255.last_seen = Instant::now();
			inner.throughput.insert(255, b255);

			// Absent track P1 (publisher already shed it): last seen 5s ago
			let mut b1 = ThroughputBucket::new();
			b1.bps = 30_000_000; // stale bps
			b1.last_seen = Instant::now() - Duration::from_secs(5);
			inner.throughput.insert(1, b1);
		}

		// CC at 5 Mbps. Without absent-track filtering, observed would be 30.1 Mbps
		// and we'd try to shed. With filtering, observed is only 100 kbps → no shed needed.
		ctrl.update(Some(5_000_000));
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 0);
	}

	#[test]
	fn no_estimate_skips_update() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		ctrl.update(None);
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 0);
	}

	#[test]
	fn cached_bps_used_for_restore() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(2), // P1 is shed
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		{
			let mut inner = ctrl.inner.lock().unwrap();
			// P150 active at 5 Mbps
			let mut b150 = ThroughputBucket::new();
			b150.bps = 5_000_000;
			b150.last_seen = Instant::now();
			inner.throughput.insert(150, b150);

			// P1 shed with cached 30 Mbps
			let mut b1 = ThroughputBucket::new();
			b1.cached_bps = 30_000_000;
			b1.last_seen = Instant::now() - Duration::from_secs(5);
			inner.throughput.insert(1, b1);

			inner.shed_since.insert(1, Instant::now() - Duration::from_secs(6));
		}

		// 40 Mbps bandwidth, but projected total = 35 Mbps.
		// Restore threshold = 35 * 1.5 = 52.5 Mbps. 40 < 52.5 → should NOT restore.
		ctrl.update(Some(40_000_000));
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 2, "should not restore: bw < projected * 1.5");

		// 60 Mbps, but BwFilter sliding-min sees min(40M, 60M) = 40M.
		// Still below 52.5M → should NOT restore yet.
		ctrl.update(Some(60_000_000));
		assert_eq!(ctrl.priority_cutoff.load(Ordering::Relaxed), 2, "BwFilter sliding-min prevents premature restore");

		// Both samples above threshold: 55M, 55M → min = 55M > 52.5M → should restore.
		// Clear the filter by providing consistently high estimates.
		// (BwFilter retains the old 40M sample within the 5s window, so we need to
		// wait it out. Instead, create a fresh controller to test the restore logic.)
		let ctrl2 = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(2),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		{
			let mut inner = ctrl2.inner.lock().unwrap();
			let mut b150 = ThroughputBucket::new();
			b150.bps = 5_000_000;
			b150.last_seen = Instant::now();
			inner.throughput.insert(150, b150);

			let mut b1 = ThroughputBucket::new();
			b1.cached_bps = 30_000_000;
			b1.last_seen = Instant::now() - Duration::from_secs(5);
			inner.throughput.insert(1, b1);

			inner.shed_since.insert(1, Instant::now() - Duration::from_secs(6));
		}

		// First sample at 55 Mbps → min = 55M > 52.5M, held long enough → restore.
		ctrl2.update(Some(55_000_000));
		assert_eq!(ctrl2.priority_cutoff.load(Ordering::Relaxed), 1, "should restore: bw > projected * 1.5");
	}

	#[test]
	fn raw_cc_sanity_prevents_false_shed() {
		let ctrl = ShedController {
			inner: Mutex::new(ShedInner {
				bw_filter: BwFilter::new(),
				throughput: HashMap::new(),
				shed_since: HashMap::new(),
				last_restore: None,
			}),
			priority_cutoff: AtomicU8::new(0),
			started_at: Instant::now() - Duration::from_secs(10),
			grace_period: Duration::ZERO,
		};

		// Simulate throughput: P1 = 30 Mbps, P255 = 100 kbps. Total ~30.1 Mbps.
		{
			let mut inner = ctrl.inner.lock().unwrap();
			let mut b1 = ThroughputBucket::new();
			b1.bps = 30_000_000;
			b1.last_seen = Instant::now();
			inner.throughput.insert(1, b1);

			let mut b255 = ThroughputBucket::new();
			b255.bps = 100_000;
			b255.last_seen = Instant::now();
			inner.throughput.insert(255, b255);
		}

		// First CC sample: low ramp-up artifact (20 Mbps < 30.1 Mbps observed).
		// BwFilter min = 20 Mbps. Would trigger shedding WITHOUT the raw check.
		ctrl.update(Some(20_000_000));
		// This sheds because raw (20M) is NOT > 2x observed (60.2M).
		assert!(ctrl.priority_cutoff.load(Ordering::Relaxed) >= 2, "low raw CC should still shed");

		// Reset cutoff for next test.
		ctrl.priority_cutoff.store(0, Ordering::Relaxed);

		// Now: raw CC is 2 Gbps (real bandwidth) but BwFilter min is still 20 Mbps
		// from the earlier sample. Without the raw check, this would falsely shed.
		ctrl.update(Some(2_000_000_000));
		assert_eq!(
			ctrl.priority_cutoff.load(Ordering::Relaxed),
			0,
			"high raw CC should prevent false shed from stale BwFilter min"
		);
	}
}

package com.openclassrooms.tourguide.tracker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openclassrooms.tourguide.service.RewardsService;
import com.openclassrooms.tourguide.service.TourGuideService;
import com.openclassrooms.tourguide.user.User;

import gpsUtil.GpsUtil;

public class Tracker extends Thread {
	private Logger logger = LoggerFactory.getLogger(Tracker.class);
	private static final long trackingPollingInterval = TimeUnit.MINUTES.toSeconds(5);
	private final GpsUtil gpsUtil;

	private final ExecutorService executorService = Executors.newSingleThreadExecutor();
	private final ExecutorService executor = Executors.newFixedThreadPool(1000);

	private final TourGuideService tourGuideService;
	private final RewardsService rewardsService;
	private boolean stop = false;

	public Tracker(TourGuideService tourGuideService, RewardsService rewardsService) {
		this.gpsUtil = new GpsUtil();
		this.tourGuideService = tourGuideService;
		this.rewardsService = rewardsService;

		executorService.submit(this);
	}

	/**
	 * Assures to shut down the Tracker thread
	 */
	public void stopTracking() {
		stop = true;
		executorService.shutdownNow();
	}

	@Override
	public void run() {
		StopWatch stopWatch = new StopWatch();
		while (true) {
			if (Thread.currentThread().isInterrupted() || stop) {
				logger.debug("Tracker stopping");
				break;
			}

			List<User> users = tourGuideService.getAllUsers();
			logger.debug("Begin Tracker. Tracking " + users.size() + " users.");
			stopWatch.start();

			for (User user : users) {

				CompletableFuture.supplyAsync(() -> {
					return gpsUtil.getUserLocation(user.getUserId());
				}, executor).thenApplyAsync(v -> user.addToVisitedLocations(v), executor)
						.thenAcceptAsync(v -> rewardsService.calculateRewards(user), executor);
			}

			stopWatch.stop();
			logger.debug("Tracker Time Elapsed: " + TimeUnit.MILLISECONDS.toSeconds(stopWatch.getTime()) + " seconds.");
			stopWatch.reset();
			try {
				logger.debug("Tracker sleeping");
				TimeUnit.SECONDS.sleep(trackingPollingInterval);
			} catch (InterruptedException e) {
				break;
			}
		}
	}
}
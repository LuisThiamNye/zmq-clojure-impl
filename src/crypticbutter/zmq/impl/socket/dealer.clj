(ns crypticbutter.zmq.impl.socket.dealer
  (:require
   [farolero.core :refer [error]]
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.pipe :as pipe]))

(defprotocol Scheduler
  (attach-pipe! [_ pipe])
  (-deactivate-pipe! [_ idx])
  (activate-pipe! [_ pipe])
  (terminate-pipe! [_ pipe]))

(defprotocol FairQueueProtocol
  (can-recv? [_])
  (recv! [_])
  (get-credential [_]))

(deftype FairQueue
         [^java.util.ArrayList pipes
          ^:unsynchronized-mutable ^:int active-count
          ^:unsynchronized-mutable ^:int current-idx
          ^:unsynchronized-mutable ^:boolean awaiting-more?
          ^:unsynchronized-mutable ^pipe/Blob last-credential]
  Scheduler
  (attach-pipe! [_ pipe]
    (.add pipes pipe)
    (.swap java.util.Collections pipes active-count (dec (.size pipes)))
    (set! active-count (inc active-count)))
  (activate-pipe! [_ pipe]
    (.swap java.util.Collections pipes (.indexOf pipes pipe) active-count)
    (set! active-count (inc active-count)))
  (-deactivate-pipe! [_ idx]
    (set! active-count (dec active-count)) ;; make inactive
    (.swap java.util.Collections pipes idx active-count) ;; swap with the first inactive pipe
    (when (== current-idx active-count) ;; terminated pipe is now at current-idx
      (set! current-idx 0)))
  (terminate-pipe! [this pipe]
    (let [idx (.indexOf pipes pipe)]
      (when (< active-count idx)
        (-deactivate-pipe! this idx))
      (.remove pipes pipe)))

  FairQueueProtocol
  (can-recv? [this]
    (or awaiting-more?
        (loop []
          (if (pos? active-count)
            (if (pipe/readable? (.get pipes current-idx))
              true
              (do (-deactivate-pipe! this current-idx)
                  (recur)))
            false))))
  (recv! [this]
    (while (pos? active-count)
      (let [current-pipe (.get pipes current-idx)
            msg (pipe/read! current-pipe)]
        (if (nil? msg)
          (do
            (set! awaiting-more? (msg/has-more? msg))
            (when-not awaiting-more?
              (set! last-credential (pipe/get-credential current-pipe))
              (set! current-idx (rem (inc current-idx) active-count))))
          (-deactivate-pipe! this current-idx)))))
  (get-credential [_] last-credential))

(defn new-fair-queue []
  (->FairQueue (java.util.ArrayList.)
               0 0 false nil))

(defprotocol LoadBalancerProtocol
  (can-send? [_])
  (send! [_ msg]))

(deftype LoadBalancer
         [^java.util.ArrayList pipes
          ^:unsynchronized-mutable ^:int active-count
          ^:unsynchronized-mutable ^:int current-idx
          ^:unsynchronized-mutable ^:boolean awaiting-more?
          ^:unsynchronized-mutable ^:boolean dropping?]
  Scheduler
  (attach-pipe! [_ pipe]
    (.add pipes pipe))
  (activate-pipe! [_ pipe]
    (.swap java.util.Collections pipes (.indexOf pipes pipe) active-count)
    (set! active-count (inc active-count)))
  (-deactivate-pipe! [_ idx]
    (set! active-count (dec active-count)) ;; make inactive
    (.swap java.util.Collections pipes idx active-count) ;; swap with the first inactive pipe
    (when (== current-idx active-count) ;; terminated pipe is now at current-idx
      (set! current-idx 0)))
  (terminate-pipe! [this pipe]
    (let [idx (.indexOf pipes pipe)]
        ;; Drop the remaining frames of a multi-part message whose pipe terminated
      (when (and (= current-idx idx) awaiting-more?)
        (set! dropping? true))
        ;; Pipe is active?
      (when (< idx active-count)
        (-deactivate-pipe! this idx))
      (.remove pipes pipe)))

  LoadBalancerProtocol
  (can-send? [this]
    (or awaiting-more?
        (loop []
          (if (zero? active-count)
            false
            (or (pipe/writable? (.get pipes current-idx))
                (do
                  (-deactivate-pipe! this current-idx)
                  (recur)))))))
  (send! [_ msg]
    (if dropping?
        ;; Continue dropping rest of message. Stop dropping if there's no more
      (do
        (set! awaiting-more? (msg/has-more? msg))
        (set! dropping? awaiting-more?))
      (do
        (while (and (pos? active-count)
                    (not (pipe/write! (.get pipes current-idx))))
          (set! active-count (dec active-count))
          (if (< current-idx active-count)
            (.swap java.util.Collections pipes current-idx active-count)
            (set! current-idx 0)))
        (when (zero? active-count)
          (error :error/conflict
                 {:message "No pipes available"}))
        (when-not (msg/has-more? msg)
          (pipe/flush! (.get pipes current-idx))
          (when (<= (dec active-count) current-idx)
            (set! current-idx 0)))))))

(defn new-load-balancer []
  (->LoadBalancer (java.util.ArrayList.)
                  0 0 false false))

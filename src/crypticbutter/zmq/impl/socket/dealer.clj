(ns crypticbutter.zmq.impl.socket.dealer
  (:require
   [farolero.core :refer [error]]
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.pipe :as pipe :refer [PipeEventSink]]
   [crypticbutter.zmq.impl.socket.base :as base :refer [Socket]])
  (:import
   (crypticbutter.zmq.impl.socket.base SocketBase)
   (crypticbutter.zmq.impl.pipe Blob)))

(defprotocol Scheduler
  (attach-pipe! [_ pipe])
  (-deactivate-pipe! [_ idx])
  (activate-pipe! [_ pipe])
  (drop-pipe! [_ pipe]))

(defprotocol FairQueueProtocol
  (can-recv? [_])
  (recv! [_])
  (get-credential [_]))

(defrecord FQRecvResult [msg pipe])

(deftype FairQueue
  [^java.util.ArrayList pipes
   ^:unsynchronized-mutable ^:int active-count
   ^:unsynchronized-mutable ^:int current-idx
   ^:unsynchronized-mutable ^:boolean awaiting-more?
   ^:unsynchronized-mutable ^Blob last-credential]
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
    (when (= current-idx active-count) ;; terminated pipe is now at current-idx
      (set! current-idx 0)))
  (drop-pipe! [this pipe]
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
    (when (pos? active-count)
      (let [current-pipe (.get pipes current-idx)
            msg (pipe/read! current-pipe)]
        (if (nil? msg)
          (do (-deactivate-pipe! this current-idx)
              (recur))
          (do
            (set! awaiting-more? (msg/has-more? msg))
            (when-not awaiting-more?
              (set! last-credential (pipe/get-credential current-pipe))
              (set! current-idx (rem (inc current-idx) active-count)))
            (->FQRecvResult msg current-pipe))))))
  (get-credential [_] last-credential))

(defn new-fair-queue []
  (->FairQueue (java.util.ArrayList.)
               0 0 false nil))

(defprotocol LoadBalancerProtocol
  (can-send? [_])
  (-send! [_ msg])
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
    (when (= current-idx active-count) ;; terminated pipe is now at current-idx
      (set! current-idx 0)))
  (drop-pipe! [this pipe]
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
  (-send! [_ msg]
    (when (pos? active-count)
      (if (pipe/write! (.get pipes current-idx) msg)
        (.get pipes current-idx)
        (do (set! active-count (dec active-count))
            (if (< current-idx active-count)
              (.swap java.util.Collections pipes current-idx active-count)
              (set! current-idx 0))
            (recur msg)))))
  (send! [this msg]
    (if dropping?
        ;; Continue dropping rest of message. Stop dropping if there's no more
      (do
        (set! awaiting-more? (msg/has-more? msg))
        (set! dropping? awaiting-more?)
        nil)
      (let [out (-send! this msg)]
        (when (zero? active-count)
          (error :error/conflict
                 {:message "No pipes available"}))
        (when-not (msg/has-more? msg)
          (pipe/flush! (.get pipes current-idx))
          (when (<= (dec active-count) current-idx)
            (set! current-idx 0)))
        out))))

(defn new-load-balancer []
  (->LoadBalancer (java.util.ArrayList.)
                  0 0 false false))

(deftype DealerSocket
  [^SocketBase socket-base
   ^LoadBalancer load-balancer
   ^FairQueue fair-queue
   ^clojure.lang.PersistentHashMap options
   ^:unsynchronized-mutable ^:boolean probe-router?]

  PipeEventSink
  (drop-pipe! [_ pipe]
    (drop-pipe! fair-queue pipe)
    (drop-pipe! load-balancer pipe))
  (activate-write! [_ pipe] (activate-pipe! load-balancer pipe))
  (activate-read! [_ pipe] (activate-pipe! fair-queue pipe))

  Socket
  (attach-pipe! [this pipe _]
    (base/with-attach-pipe! this socket-base pipe
      (when probe-router?
        (pipe/write! pipe (msg/new-msg))
        (pipe/flush! pipe))
      (attach-pipe! load-balancer pipe)
      (attach-pipe! fair-queue pipe)))
  (get-options [_] options)
  (send! [_ msg] (send! load-balancer msg))
  (recv! [_] (recv! fair-queue))
  (awaiting-more? [_] (error "not implemented")))

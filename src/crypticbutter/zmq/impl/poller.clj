(ns crypticbutter.zmq.impl.poller)

(defn rebuild-selector! []
  #_(let [old-selector selector
          new-selector (ctx.create-selector)]
      (reset! selector new-selector)
      (vreset! retired? true)
      (ctx.closeSelector old-selector)))

(defn maybe-rebuild-selector [r timeout start-timestamp]
  (let [r' (if (or (zero? timeout)
                   (< (- (System/currentTimeMillis) start-timestamp) (/ timeout 2)))
             (inc r)
             0)]
    (if (< 10 r')
      (do (rebuild-selector!)
          0)
      r')))

(defn poller [poller-state]
  (let [selector (.open Selector)]
    (fn []
      (let [counter (volatile! 0)]
        (while (:poller/running? @poller-state)
          (let [timeout 0;; TODO (execute-timers!)
                ]
            (process-retired)
            (let [start (System/currentTimeMillis)]
              (try
                (let [rc (.select selector timeout)]
                  (if (zero? rc)
                ;; If there are no events (i.e. it's a timeout) there's no point
                ;; in checking the keys.
                ;; returnsImmediately = maybeRebuildSelector (returnsImmediately, timeout, start);
                    (vreset! counter (maybe-rebuild-selector @counter timeout start))
                    (doseq [selected-key (.selectedKeys selector)]
                      (let [pollset-handle (.attachment selected-key)]
                    ;; TODO remove handle from selected keys
                        (when-not (:cancelled? pollset-handle) ;; TODO
                          (try
                            (when (.isValid selected-key)
                              (when (.isAcceptable selected-key)
                                (.acceptEvent (.-handler pollset-handle)))
                              (when (.isConenctable selected-key)
                                (.connectEvent (.-handler pollset-handle)))
                              (when (.isWritable pollset-handle)
                                (.outEvent (.-handler pollset-handle)))
                              (when (.isReadable pollset-handle)
                                (.inEvent (.-handler pollset-handle))))
                            (catch java.nio.channels.CancelledKeyException e
                              (.printStackTrace e))
                            (catch RuntimeException e
                              (.printStackTrace e))))))))
                (catch java.nio.channels.ClosedSelectorException e
              ;; rebuildSelector ();
                  (.printStackTrace e)
              ;; ctx.errno () .set (ZError.EINTR);
                  )
                (catch java.io.IOException e
              ;; throw new ZError.IOException (e);
                  )))))))))

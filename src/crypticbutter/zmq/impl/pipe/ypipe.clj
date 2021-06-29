(ns crypticbutter.zmq.impl.pipe.ypipe)

(defprotocol YQueueChunkProtocol
  (set-next! [_ v])
  (set-prev! [_ v])
  (get-next [_])
  (get-prev [_]))

(deftype YQueueChunk
         [^"[Ljava.lang.Object;" values
          ^:ints pos
          ^:unsynchronized-mutable ^YQueueChunk prev
          ^:unsynchronized-mutable ^YQueueChunk next]
  YQueueChunkProtocol
  (set-next! [_ v] (set! next v))
  (set-prev! [_ v] (set! prev v))
  (get-next! [_] next)
  (get-prev! [_] prev))

(defn- new-chunk [size memory-start]
  (->YQueueChunk (make-array Object size)
                 (let [arr (make-array Integer/TYPE size)]
                   (dotimes [i (range size)]
                     (aset arr i (+ i memory-start)))
                   arr)
                 nil nil))


(defprotocol YQueueProtocol
  (front-pos [_])
  (front [_])
  (back-pos [_])
  (back [_])
  (qconj! [_ v])
  (qdrop! [_])
  (qpop! [_]))

(deftype YQueue
         [^:unsynchronized-mutable ^YQueueChunk begin-chunk
          ^:unsynchronized-mutable ^:int begin-pos
          ^:unsynchronized-mutable ^YQueueChunk back-chunk
          ^:unsynchronized-mutable ^:int back-pos
          ^:unsynchronized-mutable ^YQueueChunk end-chunk
          ^:unsynchronized-mutable ^:int end-pos
          ^:volatile-mutable ^YQueueChunk spare-chunk
          ^:int size
          ^:unsynchronized-mutable ^:int memory-pointer]
  YQueueProtocol
  (front-pos [_]
    (aget (.-pos begin-chunk) begin-pos))
  (front [_]
    (aget (.-values begin-chunk) begin-pos))
  (back-pos [_]
    (aget (.-pos back-chunk) back-pos))
  (back [_]
    (aget (.-values back-chunk) back-pos))
  (qconj! [_ v]
    (aset (.-values back-chunk) back-pos v)
    (set! back-chunk end-chunk)
    (set! back-pos end-pos)
    (when (= end-pos (dec size))
      (if (= spare-chunk begin-chunk)
        (do
          (set-next! end-chunk (new-chunk size memory-pointer))
          (set! memory-pointer (+ size memory-pointer))
          (set-prev! (get-next end-chunk) end-chunk))
        (let [sc spare-chunk]
          (set! spare-chunk (get-next spare-chunk))
          (set-next! end-chunk sc)
          (set-prev! sc end-chunk)))
      (set! end-chunk (get-next end-chunk))
      (set! end-pos 0)))
  (qdrop! [_]
    (if (pos? back-pos)
      (set! back-pos (dec back-pos))
      (do
        (set! back-pos (dec size))
        (set! back-chunk (get-prev back-chunk))))
    (if (pos? end-pos)
      (set! end-pos (dec end-pos))
      (do
        (set! end-pos (dec size))
        (set! end-chunk (get-prev end-chunk))
        (set! (get-next end-chunk) nil))))
  (qpop! [_]
    (let [v (aget (.-values begin-chunk) begin-pos)]
      (aset (.-values begin-chunk) begin-pos nil)
      (set! begin-pos (inc begin-pos))
      (when (= size begin-pos)
        (set! begin-chunk (get-next begin-chunk))
        (set! (get-prev begin-chunk) nil)
        (set! begin-pos 0))
      v)))

(defn new-yqueue [size]
  (let [begin-chunk (new-chunk size 0)]
    (->YQueue begin-chunk
              0
              begin-chunk
              0
              begin-chunk
              1
              begin-chunk
              size
              size)))

(defprotocol YPipeProtocol
  (write! [_ v incomplete?])
  (pop-incomplete! [_])
  (flush! [_])
  (readable? [_])
  (read! [_])
  (pipe-peek [_]))

(deftype YPipe
         [^YQueue queue
          ^:unsynchronized-mutable ^:int w ;; first un-flushed
          ^:unsynchronized-mutable ^:int r ;; first un-prefetched
          ^:unsynchronized-mutable ^:int f ;; first to be flushed
     ;; last flushed item. Nil if reader is sleeping
          ^clojure.lang.Atom *c]
  YPipeProtocol
  (write! [_ v incomplete?]
    (qconj! queue v)
    (when-not incomplete?
      (set! f (back-pos queue))))
  (pop-incomplete! [_]
    (when-not (= f (back-pos queue))
      (qdrop! queue)
      (back queue)))
  (flush! [_]
    (or (= w f)
        (do (reset! *c f)
            (set! w f))))
  (readable? [_]
    (let [queue-front-pos (front-pos queue)]
      (or (not (or (= queue-front-pos r)
                   (= queue-front-pos -1)))
          (do (when-not (compare-and-set! *c queue-front-pos -1)
                (set! r @*c))
              (not (or (= queue-front-pos r)
                       (= queue-front-pos -1)))))))
  (read! [this]
    (when (readable? this)
      (qpop! queue)))
  (pipe-peek [_]
    (front queue)))

(defn new-ypipe [queue-size conflate?]
  (if conflate?
    (throw (UnsupportedOperationException. "Not implemented"))
    (let [queue (new-yqueue queue-size)
          pos (back-pos queue)]
      (->YPipe queue
               pos pos pos
               (atom pos)))))

(ns crypticbutter.zmq.impl.msg)

(def ^:const flag-more 1)
(def ^:const flag-cmd 2)
(def ^:const flag-credential 32)
(def ^:const flag-identity 64)
(def ^:const flag-shared 128)

(def ^:const type-data 0)
(def ^:const type-join 1)
(def ^:const type-leave 2)
(def ^:const type-delim 3)

(defmacro has-flag? [flags flag]
  `(= ~flag (bit-and ~flags ~flag)))

(defprotocol MsgProtocol
  (data [_])
  (has-more? [_])
  ;; (remove-flags [_])
  (credential? [_])
  (command? [_])
  (data? [_])
  (delimiter? [_])
  (identity? [_])
  (join? [_])
  (leave? [_]))

(deftype Msg
  ;; Representation of data to be sent in a single frame.
  [^java.nio.HeapByteBuffer buffer
   ^:int flags
   ^:int msg-type
   metadata
   ^:int size]

  MsgProtocol
  (data [_]
    (if (.hasArray buffer)
      (let [arr (.array buffer)
            offset (.arrayOffset buffer)]
        (cond-> arr
          (not (and (zero? offset) (= size (.-length arr))))
          (java.util.Arrays/copyOfRange offset (+ offset size))))
      (let [arr (byte-array size)]
        (doto (.duplicate buffer)
          (.position 0)
          (.get arr))
        arr)))
  (has-more? [_]
    (has-flag? flags flag-more))
  ;; (remove-flags [_ unwanted-flags]
  ;;   (bit-and flags (bit-not unwanted-flags)))
  (credential? [_]
    (has-flag? flags flag-credential))
  (command? [_]
    (has-flag? flags flag-cmd))
  (data? [_]
    (= msg-type type-data))
  (delimiter? [_]
    (= msg-type type-delim))
  (identity? [_]
    (has-flag? flags flag-identity))
  (join? [_]
    (= msg-type type-join))
  (leave? [_]
    (= msg-type type-leave)))

(defn put-uint32! [buffer value]
  (doto buffer
    (.put (byte (bit-and 255 (bit-shift-right value 24))))
    (.put (byte (bit-and 255 (bit-shift-right value 16))))
    (.put (byte (bit-and 255 (bit-shift-right value 8))))
    (.put (byte (bit-and 255 value)))))

(defn get-uint32 [buffer offset]
  (bit-or (bit-shift-left (bit-and (.get buffer offset) 255) 24)
          (bit-shift-left (bit-and (.get buffer (+ offset 1)) 255) 16)
          (bit-shift-left (bit-and (.get buffer (+ offset 2)) 255) 8)
          (bit-and (.get buffer (+ offset 3)) 255)))

(defn- new-buffer [buf-size]
  (doto (java.nio.ByteBuffer/wrap (byte-array buf-size))
    (.order java.nio.ByteOrder/BIG_ENDIAN)))

(defn new-msg
  ([] (new-msg 0 nil 0))
  ([flags]
   (new-msg flags nil 0))
  ([flags ^:bytes bytearray]
   (->Msg (doto (java.nio.ByteBuffer/wrap bytearray)
            (.order java.nio.ByteOrder/BIG_ENDIAN))
          flags
          nil
          type-data
          (.-length bytearray)))
  ([flags buf-val buf-size]
   (let [buf (new-buffer buf-size)]
     (->Msg (case buf-size
              4 (put-uint32! buf buf-val)
              0 buf)
            flags
            nil ;; metadata
            type-data
            buf-size))))

(defn new-delimiter-msg []
  (->Msg (new-buffer 0)
         0 ;; flags
         type-delim
         nil ;; Metadata
         0 ;; buffer size
         ))
(comment
  (.array (put-uint32! (doto (java.nio.ByteBuffer/wrap (byte-array 4))
                         (.order java.nio.ByteOrder/BIG_ENDIAN))
                       3))

  (let [b (byte-array 8)]
    (.putInt (java.nio.ByteBuffer/wrap b) 3)
    (java.util.Arrays/copyOfRange b 4 8))

  (defprotocol P
    (f [_ ^long x]))

  (deftype R []
    P
    (f [_ x] Long/MAX_VALUE))

  (.f (R.) "")

  (definterface I
    (g [^int x]))
  (definterface I2
    (g1 [^int x]))

  (defrecord Q []
    I
    (g [_ x] 4)
    I2
    (g1 [_ x] 7)
    P
    (f [_ x] 8))

  (.f (Q.) 4)

;;
  )

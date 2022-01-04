(ns crypticbutter.zmq.impl.socket.req
  (:require
   [farolero.core :refer [error]]
   [crypticbutter.zmq.impl.util :as util]
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.mailbox :as mbox :refer [HasMailbox]]
   [crypticbutter.zmq.impl.ctx :as ctx]
   [crypticbutter.zmq.impl.cmd :as cmd]
   [crypticbutter.zmq.impl.pipe :as pipe :refer [PipeEventSink]]
   [crypticbutter.zmq.impl.session :as sess]
   [crypticbutter.zmq.impl.socket.base :as base]
   [crypticbutter.zmq.impl.socket.dealer :as dealer]
   [crypticbutter.zmq.impl.poller :as poller])
  (:import
   (crypticbutter.zmq.impl.socket.base SocketBase)
   (crypticbutter.zmq.impl.pipe Pipe)))

(defmacro cstr [& strs]
  (apply str strs))

(derive ::error.pending-reply :error/conflict) ;; EFSM
(derive ::error.no-pending-reply :error/conflict) ;; EFSM

(deftype ReqSession
  [^:volatile-mutable ^clojure.lang.Keyword phase  ;; bottom, req-id, body
   ^:unsynchronized-mutable ^Pipe pipe]

  sess/SessionBase
  (reset-state! [_] (set! phase :bottom))
  (attach-pipe! [_ new-pipe] (set! pipe new-pipe))
  (push-msg! [_ msg]
    (when-not (msg/command? msg)
      (if (nil? (case phase
                  :bottom
                  (when (msg/has-more? msg)
                    (cond
                      (== 4 (.-size msg)) (set! phase :req-id)
                      (zero? (.-size msg)) (set! phase :body)))
                  :req-id
                  (when (and (msg/has-more? msg)
                             (zero? (.-size msg)))
                    (set! phase :body))
                  :body
                  (or (msg/has-more? msg)
                      (when (zero? (.-flags msg))
                        (set! phase :bottom)))))

        (error :error/fault {})
        (pipe/write! pipe msg)))))

(defn new-req-session []
  (->ReqSession :bottom ;; phase
                nil ;; pipe
                ))

(defprotocol Req
  (-reset! [_])
  (recv-reply-pipe! [_]))

(deftype ReqSocket
  [^SocketBase socket-base
   ^:volatile-mutable ^:boolean sending?
   ^:volatile-mutable ^:boolean awaiting-initial?
   ^:volatile-mutable ^Pipe reply-pipe
   ^:volatile-mutable ^:int last-request-id
   ^:volatile-mutable ^clojure.lang.Keyword matching-rule
   ^:volatile-mutable ^clojure.lang.PersistentHashMap endpoints
   load-balancer
   fair-queue
   options]

  Req
  (-reset! [_]
    (set! sending? true)
    (set! awaiting-initial? true))
  (recv-reply-pipe! [this]
    (when-some [{:keys [msg pipe]} (dealer/recv! fair-queue)]
      (if (or (nil? reply-pipe)
              (= reply-pipe pipe))
        msg
        (recur))))

  base/Socket
  (get-options [_] options)
  (awaiting-more? [_] (not awaiting-initial?))
  (attach-pipe! [this pipe _]
    (base/with-attach-pipe! this socket-base pipe
      (dealer/attach-pipe! load-balancer pipe)
      (dealer/attach-pipe! fair-queue pipe)))
  (recv! [this]
    (if sending?
      (error ::error.no-pending-reply
             {:message "Cannot listen for a new reply because there is no outstanding requst."})
      (when-some
       [msg (and (not= :return
                       (when awaiting-initial?
                         (set! awaiting-initial? false)
                         (loop []
                           (let [r (or (when (= :frames matching-rule)
                                         (if-some [msg (recv-reply-pipe! this)]
                                           (when-not (and (msg/has-more? msg)
                                                          (== 4 (.-size msg))
                                                          (= last-request-id (msg/get-uint32 (.-buffer msg) 0)))
                                             (while (msg/has-more? msg)
                                               (recv-reply-pipe! this))
                                             :recur)
                                           :return))
                                       (if-some [msg (recv-reply-pipe! this)]
                                         (when-not (and (msg/has-more? msg) (zero? (.-size msg)))
                                           (while (msg/has-more? msg)
                                             (recv-reply-pipe! this))
                                           :recur)
                                         :return))]
                             (case r :recur (recur) r)))))
                 (recv-reply-pipe! this))]
        (when-not (msg/has-more? msg)
          (-reset! this)))))
  (send! [this msg]
    (when-not sending?
      (if (= :alternation matching-rule)
        (error ::error.pending-reply
               {:message
                (cstr "Cannot send a new request whilst awaiting a reply to a prior "
                      "request, because the strict alternation matching rule is in use.")})
        (-reset! this)))
    (when awaiting-initial?
      (when (= :frames matching-rule)
        (let [request-id (inc last-request-id)]
          (dealer/send! load-balancer (msg/new-msg msg/flag-more request-id 4))
          (set! last-request-id request-id)))
      (let [delim-msg (msg/new-msg msg/flag-more)]
        (set! reply-pipe (dealer/send! load-balancer delim-msg))
        (set! awaiting-initial? false)
        ;; Drop remaining messages
        (while (some? (dealer/recv! fair-queue)))))
    (dealer/send! load-balancer msg)
    (when-not (msg/has-more? msg)
      (set! awaiting-initial? true)
      (set! sending? false)))

  PipeEventSink
  (hiccup! [_ pipe]
    (if (:only-fill-connected? options)
      (error "IDK")
      (pipe/terminate! pipe false)))
  (activate-read! [_ pipe] (dealer/activate-pipe! fair-queue pipe))
  (activate-write! [_ pipe] (dealer/activate-pipe! load-balancer pipe))
  (drop-pipe! [_ pipe]
    (when (= pipe reply-pipe)
      (set! reply-pipe nil))
    (dealer/drop-pipe! fair-queue pipe)
    (dealer/drop-pipe! load-balancer pipe)
    (base/remove-pipe! socket-base pipe)))

(def req-creator
  (reify base/SocketCreator
    (create-socket [_ id]
      (->ReqSocket true true                   ;; state machine
                   nil                         ;; reply pipe
                   0                           ;; last request id
                   :alternation                ;; matching rule
                   {}                          ;; endpoints
                   (dealer/new-load-balancer)
                   (dealer/new-fair-queue)
                   {} ;; TODO socket options
                   ))))

(comment
  (defn b [x]
    (if (< 10 x)
      (throw (ex-info "" {}))
      (recur (inc x))))

  (b 0)

  (loop []
    (or 4
        (recur)
        8))

  ;;
  )

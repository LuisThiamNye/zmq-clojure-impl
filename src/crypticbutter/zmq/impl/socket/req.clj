(ns crypticbutter.zmq.impl.socket.req
  (:require
   [crypticbutter.zmq.impl.msg :as msg]
   [crypticbutter.zmq.impl.socket.base :as base]
   [crypticbutter.zmq.impl.socket.dealer :as dealer]))

(defmacro cstr [& strs]
  (apply str strs))

(derive ::error.pending-reply :error/conflict) ;; EFSM
(derive ::error.no-pending-reply :error/conflict) ;; EFSM

(defrecord ReqState
           [sending?
            awaiting-initial?
            reply-pipe
            request-id])

[:map {:closed false}
 [::sending? boolean?]
 [::awaiting-initial? boolean?]
 [::reply-pipe :any]
 [::request-id int?]]

(defn reset-req
  [state]
  (assoc state
         ::sending? true
         ::awaiting-initial? true))

(defn recv! [_]
  (when (:sending? @*state)
    (error ::error.no-pending-reply
           {:message "Cannot listen for a new reply because there is no outstanding requst."})))

(defn- send-req-identity! [state]
  (if (:awaiting-initial? state)
    (let [lb (::load-balancer state)
          state (if (= :frames matching-rule)
                  (let [request-id (inc (::request-id state))]
                    (dealer/send! lb (msg/new-msg msg/flag-more request-id 4))
                    (assoc state ::request-id request-id))
                  state)
          delim-msg (msg/new-msg msg/flag-more)]
      (dealer/send! lb delim-msg)
      (assoc state ::awaiting-initial? false))
    state))

(defn send! [state]
  (let [matching-rule (::matching-rule state)]
    (when-not (:sending? state)
      (if (= :alternation matching-rule)
        (error ::error.pending-reply
               {:message
                (cstr "Cannot send a new request whilst awaiting a reply to a prior "
                      "request, because the strict alternation matching rule is in use.")})
        (recur (reset-req state))))
    (let [state (send-req-identity! state)]))
  drop-messages)

(defn new-req []
  {::sending? true
   ::awaiting-initial? true
   ::reply-pipe nil
   ::request-id 0
   ::matching-rule :alternating
   ::load-balancer (dealer/new-load-balancer)
   ::fair-queue (dealer/new-fair-queue)})

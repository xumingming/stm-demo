(ns stm-demo.core
  (import [java.io FileWriter]))

;; Clojure STM API
;; dosync划定事务边界
(let [r1 (ref 1)
      r2 (ref 1)
      r3 (ref 1)
      r4 (ref 1)
      r5 (ref 1)]
  (dosync
   ;; 把r1修改成给定值
   (ref-set r1 1)
   ;; 以r2的当前值作为参数调用inc，
   ;; 返回值作为r2的新值
   (alter r2 inc)
   ;; 用法跟alter类似，以r3当前值
   ;; 来调用inc，返回值作为r3新值
   ;; 并发性能比ref-set, alter
   ;; 要好，但是不是所有场景可用
   (commute r3 inc)
   ;; 确保在当前事务执行过程中，r4
   ;; 不会被别的事务修改
   (ensure r4)
   ;; 获取r5的事务内的值
   @r5))

;; 银行转账例子: 从a转10元到b
(let [a (ref 50)
      b (ref 50)]
  (dosync (alter a + 10)
          (alter b - 10))
  [@a @b])

;; ref-set冲突，同一时间只有一个事务能获取到一个ref
;; 的锁，从而可以对ref进行修改，其它的事务则必须重试
(let [r (ref 1)
      t1 (future (dosync_xumm ["t1"]
                  (Thread/sleep 1000)                     
                  (ref-set r 101)))
      t2 (future (dosync_xumm ["t2"]
                  (Thread/sleep 5000)
                  (ref-set r 201)))]
  [@t1 @t2 @r])


;; commute不冲突，多个事务可以同时操作一个ref
;; 事务也不会重试
(let [r (ref 1)
      t1 (future (dosync
                  (Thread/sleep 1000)                     
                  (commute r + 100)))
      t2 (future (dosync
                  (Thread/sleep 5000)
                  (commute r + 200)))]
  [@t1 @t2 @r])

;; commute比alter(ref-set)的并发性能要好
(time (let [r (ref 1)
            futures (for [i (range 100)]
                      (future (dosync_xumm [(str i)]
                               (alter r inc))))]
        (doseq [f futures]
          (deref f))
        @r))

(time (let [r (ref 1)
            futures (for [i (range 100)]
                      (future (dosync_xumm [(str i)]
                                      (commute r inc))))]
        (doseq [f futures]
          (deref f))
        @r))


;; ensure保证ref在事务执行过程中不被修改
(let [r (ref 1)
      f1 (future (dosync_xumm ["f1"]
                  (ensure r)
                  (Thread/sleep 1000)))
      
      f2 (future (dosync_xumm ["f2"]
                  (ref-set r 2)))]
  [@f1 @f2])

;; 一个再长的事务都不会被"活锁"
(let [fw (FileWriter. "/tmp/stm.log")
      r (ref 1)
      exit? (atom false)]
  (binding [*out* fw]
    ;; 添加一个watcher，这样在LONG这个事务执行
    ;; 完成的时候会把exit?置为true
    (add-watch r :key
               (fn [k tr old-value new-value]
                 (when (= "LONG-VALUE" new-value)
                   (println "Long transaction successfully executed!")
                   (reset! exit? true))))


    ;; 执行一个很长的事务
    (future (dosync_xumm ["LONG"]
                         (ref-set r "LONG-VALUE")
                         (Thread/sleep 2000)))
  
    ;; 不断的生成新的事务，直到exit?为true
    (loop [i 0]
      (when-not @exit?
        (future (dosync_xumm [(str i)]
                             (ref-set r i)
                             (Thread/sleep 100)))
        (Thread/sleep 100)
        (recur (inc i))))))


;; 一个再长的事务都不会被"活锁"
;; 这里的长是指这个事务需要操作很多ref
(let [fw (FileWriter. "/tmp/stm.log")
      r (ref 1)
      r1 (ref 1)
      r2 (ref 2)
      r3 (ref 3)
      exit? (atom false)
      exit1? (atom false)
      exit2? (atom false)
      exit3? (atom false)]
  (binding [*out* fw]
    ;; 添加一个watcher，这样在LONG这个事务执行
    ;; 完成的时候会把exit?置为true
    (add-watch r :key
               (fn [k tr old-value new-value]
                 (when (= "LONG-VALUE" new-value)
                   (println "Long transaction successfully executed!")
                   (reset! exit? true))))

    (add-watch r1 :key
               (fn [k tr old-value new-value]
                 (when (= "LONG-VALUE" new-value)
                   (println "Long transaction1 successfully executed!")
                   (reset! exit1? true))))

    (add-watch r2 :key
               (fn [k tr old-value new-value]
                 (when (= "LONG-VALUE" new-value)
                   (println "Long transaction2 successfully executed!")
                   (reset! exit2? true))))

    (add-watch r3 :key
               (fn [k tr old-value new-value]
                 (when (= "LONG-VALUE" new-value)
                   (println "Long transaction3 successfully executed!")
                   (reset! exit3? true))))


    ;; 执行一个很长的事务
    (future (dosync_xumm ["LONG"]
                         (ref-set r "LONG-VALUE")
                         (ref-set r1 "LONG-VALUE")
                         (ref-set r2 "LONG-VALUE")
                         (ref-set r3 "LONG-VALUE")
                         (Thread/sleep 2000)))
  
    ;; 不断的生成新的事务，直到exit?为true
    (loop [i 0]
      (when-not @exit?
        (future (dosync_xumm [(str i)]
                             (ref-set r i)
                             (Thread/sleep 100)))
        (Thread/sleep 100)
        (recur (inc i))))

    ;; 不断的生成新的事务，直到exit1?为true
    (loop [i 0]
      (when-not @exit?
        (future (dosync_xumm [(str i)]
                             (ref-set r1 i)
                             (Thread/sleep 100)))
        (Thread/sleep 100)
        (recur (inc i))))

    ;; 不断的生成新的事务，直到exit2?为true
    (loop [i 0]
      (when-not @exit?
        (future (dosync_xumm [(str i)]
                             (ref-set r2 i)
                             (Thread/sleep 100)))
        (Thread/sleep 100)
        (recur (inc i))))

    ;; 不断的生成新的事务，直到exit3?为true
    (loop [i 0]
      (when-not @exit?
        (future (dosync_xumm [(str i)]
                             (ref-set r3 i)
                             (Thread/sleep 100)))
        (Thread/sleep 100)
        (recur (inc i))))))


;; 只读事务也可能会重试
(let [r (ref 1 :min-history 1)
      f1 (future (dosync (Thread/sleep 1000)
                         @r))
      f2 (future (dosync (ref-set r 2)))
      ]x
  [@f1 @f2])

;; 探查ref-history-count
(let [r (ref 1)
      f1 (future (dosync_xumm ["f1"]
                  (Thread/sleep 100)
                  (ref-set r 1)))
      f2 (future (dosync_xumm ["f2"]
                  (Thread/sleep 200)
                  (ref-set r 2)))]
  [@f1 @f2]
  (println "ref-history-count: " (ref-history-count r)))

;; 由于min-history > 0，所以ref-history-count会增加
(let [r (ref 1 :min-history 1)]
  @(future (dosync_xumm ["f1"]
                        (Thread/sleep 1000)
                        (ref-set r :f1)
                        @r))
  (println (ref-history-count r)))

;; 由于有读失败，所以ref-history-count会增加
(let [r (ref 1)
      f1 (future (dosync_xumm ["f1"]
                              (Thread/sleep 1000)
                              @r))
      f2 (future (dosync_xumm ["f2"]
                              (ref-set r 2)))
      f3 (future (dosync_xumm ["f3"]
                              (Thread/sleep 1500)
                              (ref-set r 2)))]
  [@f1 @f2 @f3]
  (println "ref-history-count: " (ref-history-count r)))


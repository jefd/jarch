(import ./tk)
(import sqlite3 :as sql)
(import json)
(import http)

(def db-path "./gh2.db")
(def token tk/gh-token)

(def sleep-time 5)
(def max-tries 20)

(def repos  [
        {:owner "ufs-community" :name "ufs-weather-model" :token token }
        {:owner "ufs-community" :name "ufs-srweather-app" :token token }
        {:owner "ufs-community" :name "ufs-mrweather-app" :token token }
        {:owner "ufs-community" :name "regional_workflow" :token token }
])

# map of metric name to github api path
(def metrics {:views "/traffic/views"
              :clones "/traffic/clones"
              :frequency "/stats/code_frequency"
              :commits "/commits?per_page=100&page=1"
              :forks "/forks?per_page=100&page=1"
             })


```
(defmacro with-db [db-path db-name & body]
  ~(with [,db-name (sql/open ,db-path) (fn [,db-name] (sql/close ,db-name))]
     ,;body
     (sql/close ,db-name)))
```

(defmacro with-db [db-path db-name & body]
  ~(with [,db-name (sql/open ,db-path) (fn [,db-name] (sql/close ,db-name))]
     (def ret (do ,;body))
     (sql/close ,db-name)
     ret))


(defn ok? [r]
  (= (r :status) 200))

(defn qw [name]
  (string `"` name `"`))

(defn exec-query [db-path query &opt vals]
  (default vals {})
  (with [db (sql/open db-path) (fn [db] (sql/close db))]
    (def rows (sql/eval db query vals))
    (sql/close db)
    rows))

(defn mget [url headers]
  
  (defn mget-helper [tries]
    (print "Requesting " url)
    (def r (http/get url :headers headers))
    (cond
      (ok? r) 
      (do 
        (print "Received response! Tries = " tries)
        r)
      (>= tries max-tries) 
      (do 
        (print "Exceeded maximum tries of " max-tries)
        nil)
      (do
        (print "Status code = " (r :status))
        (print "Tries = " tries " out of " max-tries)
        (print "Going to sleep for " sleep-time " seconds...")
        (os/sleep sleep-time)
        (mget-helper (+ tries 1)))))

  (mget-helper 1))

(defn get-latest [table-name]
  (let [tbl (string `"` table-name `"`)
        query (string "select timestamp from " tbl " order by timestamp desc limit 1;")
        rows (exec-query db-path query)]

    (if (>= (length rows) 0) (rows 0) nil))) 

(defn prune-list [lst latest]
  (if (not latest)
    lst
    (filter (fn [st] (> (st :timestamp) latest))  lst)))

(defn get-table-name [repo metric]
  (let [owner (repo :owner)
        repo-name (repo :name)]

    (string owner "/" repo-name "/" metric)))

(defn create-repo-table [db-path]
  (let [q `create table if not exists repos (
          owner text not null,
          name text not null,
          metric text not null,
          minDate text not null);`]

    (exec-query db-path q)))

(defn create-metric-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 (qw table-name) 
                 `(timestamp text not null,
                   count integer not null,
                   uniques integer not null);`)]
    
    (exec-query db-path q)))

(defn create-freq-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 (qw table-name) 
                 `(timestamp text not null,
                   additions integer not null,
                   deletions integer not null);`)]
    
    (exec-query db-path q)))

(defn create-commit-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 (qw table-name) 
                 `(timestamp text not null,
                   commits integer not null);`)]
    
    (exec-query db-path q)))

(defn create-fork-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 (qw table-name) 
                 `(fork_count integer not null);`)]
    
    (exec-query db-path q)))


(defn multi-insert [db-path table-name query lst]
  (with [db (sql/open db-path) (fn [db] (sql/close db))]
    (each dct lst
      (print "Inserting into " (qw table-name) ": " (json/encode dct))
      (sql/eval db query dct))
    (sql/close db)))

(defn insert-metrics [db-path table-name lst]
  (def query (string  "insert into " (qw table-name) " values (:timestamp, :count, :uniques);"))
  (multi-insert db-path table-name query lst))

(defn insert-commits [db-path table-name lst]
  (def query (string  "insert into " (qw table-name) " values (:timestamp, :commits);"))
  (multi-insert db-path table-name query lst))

(defn insert-frequency [db-path table-name lst]
  (def query (string  "insert into " (qw table-name) " values (:timestamp, :additions, :deletions);"))
  (multi-insert db-path table-name query lst))

(defn insert-or-update-forks [db-path table-name fork-count]
  (def insert-query (string  "insert or ignore into forks values (:fork_count);"))
  (def update-query (string "update forks set fork_count = :fork_count;" ))

  (with [db (sql/open db-path) (fn [db] (sql/close db))]
    (def rows (sql/eval db "select count(*) from forks;"))
    (def row (rows 0))
    (def key ((keys row) 0))
    (def cnt (row key))
    (def query (if (= 0 cnt) insert-query update-query))
    (sql/eval db query {:fork_count fork-count})
    (sql/close db)))


(defn get-url [repo metric]
  (let [owner (repo :owner)
        repo-name (repo :name)
        path (metrics metric)]

    (string "https://api.github.com/repos/" owner "/" repo-name path)))
    #"http://jsonplaceholder.typicode.com/users"))

(defn get-headers [repo]
  (let [token (repo :token)]

    {:Accept "application/vnd.github.v3+json"
     :User-Agent "epic"
     :Authorization (string "token " token)
    }

    ))

(defn get-views [repo]
  (let [url (get-url repo :views)
        headers (get-headers repo)
       ]
    
    (def r (mget url headers))

    (if r 
      (json/decode (r :body)))))

(defn get-clones [repo]
  (let [url (get-url repo :clones)
        headers (get-headers repo)
       ]
    
    (def r (mget url headers))

    (if r 
      (json/decode (r :body)))))

(defn get-metrics [repo metric]
  (let [url (get-url repo metric)
        headers (get-headers repo)
       ]
    
    (print "Getting metrics from " url)
    (def r (mget url headers))

    (if r 
        ((json/decode (r :body)) (string metric)))))

```
(defn split-strip [delim s]
  (filter (fn [elt] (not (= elt ""))) 
          (map string/trim (string/split delim s))))
```

# using a fancy threading macro
(defn split-strip [delim s]
  (->> s
      (string/split delim)
      (map string/trim)
      (filter (fn [elt] (not (= elt ""))))))

(defn get-links [headers]
  (def rel-list ["first" "last" "next" "prev"])
  (def m @{})
  (def link (get headers "link"))
  (if (not link) (break nil))
  (each rel rel-list
    (def l (split-strip "," link))
    (def l2 (map (partial split-strip ";") l))
    (each [url relative] l2
      (if (string/find rel relative)
        (put m rel (string/slice url 1 -2)))))
  m)


(defn get-fork-count [repo]
  (defn get-count [lst]
    (var total 0)
    (each l lst
      (let [fork-count (l "forks_count")]
        (set total (+ total 1 fork-count))))
    
    total)

  (def headers (get-headers repo))
  (var url (get-url repo :forks))
  (var total 0)

  (while url
    (let [r (mget url headers)]
      #(if (= (r :status) 200)
      (if (ok? r)
        (let [lst (json/decode (r :body))
              links (get-links (r :headers))]

          (set total (+ total (get-count lst)))
          (set url (get links "next"))))))

  total)

(defn get-commits [repo]

  (defn add-commits [lst commit-dct]
    (each l lst
      (let [date (-> l
                     (get "commit")
                     (get "author")
                     (get "date")
                     (string/slice 0 10)
                     (string "T00:00:00Z"))
            n (get commit-dct date)]

        (if n
          (put commit-dct date (+ 1 n))
          (put commit-dct date 1)))))

  (defn get-sorted-list [dct]
    (let [ret-list @[]]
      (each k (sorted (keys dct))
        (array/push ret-list {"timestamp" k "commits" (get dct k)}))

      ret-list))

  (def headers (get-headers repo))
  (var url (get-url repo :commits))
  (var commit-dct @{})

  (while url
    (let [r (mget url headers)]
      (if (ok? r)
        (let [lst (json/decode (r :body))
              links (get-links (r :headers))]

          (add-commits lst commit-dct)
          (set url (get links "next"))))))

  (get-sorted-list commit-dct))


(defn update-repo-table [repo metric]
  (def owner (repo :owner))
  (def name (repo :name))
  (def table-name (get-table-name repo metric))
  (def select-query (string "select timestamp from " (qw table-name) " order by timestamp limit 1;"))
  (def insert-query (string "insert into repos values (:owner :name :metric :minDate);"))

  (with [db (sql/open db-path) (fn [db] (sql/close db))]
    (def rows (sql/eval db select-query))
    (def min-date (rows 0))
    (sql/eval db insert-query {:owner owner :name name :metric metric :midDate min-date})
    (sql/close db)))

(defn row-exists? [repo metric]
  (let [owner (repo :owner)
        name (repo :name)
        query (string "select minDate from repos where owner=" (qw owner) " and name=" (qw name) " and metric=" (qw metric) ";")
        rows (exec-query db-path query)
        ]

    (rows 0)))

```
def row_exists(con, repo, metric):
    cursor = con.cursor()
    owner = repo['owner']
    name = repo['name']
    sql = f'''select minDate from repos where owner="{owner}" and name="{name}" and metric="{metric}";'''
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    return result
```

(defn to-double-digit-string [digit]
  (string/slice (string "0" digit) -3))

(defn get-date-time-string [time]
  (let [date (os/date time)
        year (get date :year)
        #month0 (to-double-digit-string (get date :month))
        month (to-double-digit-string (+ 1 (get date :month)))
        #day0 (to-double-digit-string (get date :month-day))
        day (to-double-digit-string (+ 1 (get date :month-day)))
        hours (to-double-digit-string (get date :hours))
        minutes (to-double-digit-string (get date :minutes))
        seconds (to-double-digit-string (get date :seconds))]
    (string year "-" month "-" day "T" hours ":" minutes ":" seconds "Z")))

(def to-date get-date-time-string)

(defn get-frequency [repo]
  (let [url (get-url repo :frequency)
        headers (get-headers repo)
        r (mget url headers)]
    
    (if (ok? r) 
      (let [lst (json/decode (r :body))]
        # return list of structs
        (map 
          (fn [l]
            {:timestamp (to-date (l 0)) :additions (l 1) :deletions (l 2)})
          lst)))))

(defn main [&]
  (def tbl (get-table-name (repos 0) "views"))
  (def latest "2023-01-01")
  (def lst [
             {:timestamp "2022-05-05"}
             {:timestamp "2023-01-02"}
             {:timestamp "2023-01-03"}
            ])

  (def lst2 [
              {:timestamp "2022-05-05" :count 5 :uniques 3}
              {:timestamp "2023-01-02" :count 7 :uniques 4}
              {:timestamp "2023-02-06" :count 12 :uniques 5}
            ])

  (def lst3 [
              {:timestamp "2022-05-05" :commits 5}
              {:timestamp "2023-01-02" :commits 7}
              {:timestamp "2023-02-06" :commits 10}
            ])

  (def lst4 [
              {:timestamp "2022-05-05" :additions 5 :deletions 10}
              {:timestamp "2023-01-07" :additions 20 :deletions 5}
              {:timestamp "2023-03-09" :additions 23 :deletions 30}
            ])

  #(pp (get-latest tbl))
  #(pp (prune-list lst latest))
  #(pp (get-table-name (repos 0) "views"))
  #(pp (create-repo-table db-path))
  #(pp (create-metric-table db-path "views"))
  #(pp (create-freq-table db-path "frequency"))
  #(pp (create-commit-table db-path "commits"))
  #(pp (create-fork-table db-path "forks"))

  #(insert-metrics db-path "views" lst2)
  #(insert-commits db-path "commits" lst3)
  #(insert-frequency db-path "frequency" lst4)
  #(insert-or-update-forks db-path "forks" 200)
  #(insert-or-update-forks db-path "forks" 300)
  #(print (get-url (repos 0) :views))
  #(pp (get-headers (repos 0)))
  #(pp (get-views (repos 0)))
  #(def c (get-clones (repos 0)))
  #(pp c)
  #(print "count: " (c "count"))
  #(def m (get-metrics (repos 0) :views))
  #(pp m)
  #(def f (get-frequency (repos 0)))
  #(each elt f
  #  (pp elt))
  #(print (get-fork-count-r (repos 0)))
  #(def commits (get-commits (repos 0)))
  #(each c commits
  #  (pp c))
  
  )



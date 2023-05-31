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

(defn exec-query [db-path query]
  (let [db (sql/open db-path)
        rows (sql/eval db query)]
    (sql/close db)
    rows))

(defn mget [url headers]
  
  (defn mget-helper [tries]
    (print "Requesting " url)
    (def r (http/get url :headers headers))
    (cond
      (= (r :status) 200) 
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
                 table-name 
                 `(timestamp text not null,
                   count integer not null,
                   uniques integer not null);`)]
    
    (exec-query db-path q)))

(defn create-freq-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(timestamp text not null,
                   additions integer not null,
                   deletions integer not null);`)]
    
    (exec-query db-path q)))

(defn create-commit-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(timestamp text not null,
                   commits integer not null);`)]
    
    (exec-query db-path q)))

(defn create-fork-table [db-path table-name]
  (let [q (string `create table if not exists ` 
                 table-name 
                 `(fork_count integer not null);`)]
    
    (exec-query db-path q)))

(defn insert-metrics [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "Inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :count, :uniques);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "Inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))

(defn insert-commits [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "Inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :commits);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "Inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))

(defn insert-frequency [db-path table-name lst]
  (if (not lst)
    nil
    (do
      (print "Inserting metrics into " table-name)

      (def insert-query (string  "insert into " table-name " values (:timestamp, :additions, :deletions);"))

      (def db (sql/open db-path))

      (each dct lst
        (print "Inserting " (json/encode dct))
        (sql/eval db insert-query dct))

      (sql/close db))))

(defn insert-or-update-forks [db-path table-name fork-count]

  (let [db (sql/open db-path)
        insert-query (string  "insert or ignore into forks values (:fork_count);")
        update-query (string "update forks set fork_count = :fork_count;" )
        rows (sql/eval db "select count(*) from forks;")
        row (rows 0)
        key ((keys row) 0)
        cnt (row key)
        query (if (= 0 cnt) insert-query update-query)]

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
  (def link (get headers "Link"))
  (if (not link) (break nil))
  (each rel rel-list
    (def l (split-strip "," link))
    (def l2 (map (partial split-strip ";") l))
    (each [url relative] l2
      (if (string/find rel relative)
        (put m rel (string/slice url 1 -2)))))
  m)

(defn get-fork-count [repo]
  (var url (get-url repo :forks))
  (def headers (get-headers repo))

  (var total 0)

  (while url
    (def r (mget url headers))
    (if (= (r :status) 200)
      (do
        (def lst (json/decode (r :body)))
        (each l lst
          (def count (l "forks_count"))
          (if (= count 0)
            (set total (+ total 1))
            (set total (+ total count 1))))
       
        (def links (get-links (r :headers)))

        (set url (get links "next")))))

  total)

```
# recursive version
(defn get-fork-count [repo]
  (defn get-fork-count-helper [url headers total]
    (if (not url)
      total
      (do
        (def r (mget url headers))
        (if (= (r :status) 200)
          (do
            (def lst (json/decode (r :body)))
            (var new-total total)
            (each l lst
              (def count (l "forks_count"))
              (if (= count 0)
                (set new-total (+ new-total 1))
                (set new-total (+ count 1))))

            (def links (get-links (r :headers)))
            (def url (get links "next"))

            (get-fork-count-helper url headers new-total))))))

  (def url (get-url repo :forks))
  (def headers (get-headers repo))

  (get-fork-count-helper url headers 0)) 
```





```
def get_fork_count(repo):
    url = get_url(repo, 'forks')
    headers = get_headers(repo)

    total = 0
    while url:
        #r = requests.get(url, headers=headers)
        r = mget(url, headers)
        if r.status_code == 200:
            lst = json.loads(r.content)

            for l in lst:
                count = l['forks_count']
                if count == 0:
                    total += 1
                else:
                    total += (count + 1)


            links = get_links(r.headers)

            url = links.get('next')

    return total
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
       ]
    
    (def r (mget url headers))

    (if r 
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
  #(insert-or-update-forks db-path "forks" 5)
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
  (print (get-fork-count (repos 0)))
  
  )



(import ./tk)
(import sqlite3 :as sql)

(def db-path "./gh2.db")
(def token tk/gh-token)

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


(defn get-table-name [repo metric]
  (let [owner (repo :owner)
        repo-name (repo :name)
        ]

    (string owner "/" repo-name "/" metric)))


(defn exec-query [db-path query]
  (try
    (let [db (sql/open db-path)
          rows (sql/eval db query)]

      (sql/close db)
      rows)
    
  ([err] (print "sql error: " err " query: " query))))


(defn get-latest [table-name]
  (let [tbl (string `"` table-name `"`)
        query (string "select timestamp from " tbl " order by timestamp desc limit 1;")
        rows (exec-query db-path query)
        ]

    (if (>= (length rows) 0) (rows 0) nil))) 


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


(defn prune-list [lst latest]
  (if (not latest)
    lst
    (filter (fn [st] (> (st :timestamp) latest))  lst)))
    


            
(defn main [&]
  (def tbl (get-table-name (repos 0) "views"))
  (def latest "2023-01-01")
  (def lst [
            {:timestamp "2022-05-05"}
            {:timestamp "2023-01-02"}
            {:timestamp "2023-01-03"}
            ])

  #(pp (get-latest tbl)))
  #(pp (prune-list lst latest)))
  #(pp (get-table-name (repos 0) "views")))
  (pp (create-repo-table db-path)))
  #(pp (create-metric-table db-path "testtable")))




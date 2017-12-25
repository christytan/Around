package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"github.com/pborman/uuid"
	"fmt"
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"reflect"
	"cloud.google.com/go/bigtable"
	"context"
)

const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	// Needs to update
	//PROJECT_ID = "around-xxx"
	//BT_INSTANCE = "around-post"
	// Needs to update this URL if you deploy it to cloud.
	ES_URL = "http://54.200.111.216:9200"
	PROJECT_ID = "around-190005"
	BT_INSTANCE = "around-post"

)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string `json:"user"`
	Message  string  `json:"message"`
	Location Location `json:"location"`
	Url      string   `json:"url"`
}



func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
                    "mappings":{
                           "post":{
                                  "properties":{
                                         "location":{
                                                "type":"geo_point"
                                         }
                                  }
                           }
                    }
             }
             `
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	fmt.Println("start service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))

}


func handlerPost(w http.ResponseWriter, r *http.Request) {
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post

	if err := decoder.Decode(&p); err != nil {
		panic(err)
		return
	}

	//fmt.Printf(w, "Post received: %s\n", p.Message) //return to the response(client side)

	//recieve client post json obj and store it to the elastic search

	// Create a client - create connection to elastic search
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	id := uuid.New()
	_, err = client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}


	ctx := context.Background()
	// you must update project name here

	bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE)
	if err != nil {
		panic(err)
		return
	}

	// TODO (student questions) save Post into BT as well


	tbl := bt_client.Open("post")
	mut := bigtable.NewMutation()
	mut.Set("post", "user", bigtable.Now(), []byte(p.User))
	mut.Set("post", "message", bigtable.Now(), []byte(p.Message))
	mut.Set("location", "location.lat", bigtable.Now(), []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
	mut.Set("location", "location.lon", bigtable.Now(), []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))
	err = tbl.Apply(ctx, "com.google.cloud", mut)
	if err != nil {
		panic(err)
		return
	}
	fmt.Printf("Post is saved to BigTable: %s\n", p.Message)




}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	//fmt.Println("Received one Search request");
	//
	//lat := r.URL.Query().Get("lat")
	//lon := r.URL.Query().Get("lon")
	//fmt.Printf( "Search received: %s %s", lat, lon)

	//following is the response back to the user
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	// range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received: %f %f %s", lat, lon, ran)

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true).
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post) // p = (Post) item
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		// TODO(student homework): Perform filtering based on keywords such as web spam etc.
		ps = append(ps, p)

	}
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}



	w.Header().Set("Content-Type", "application/json")//tell browser what is your return type
	w.Write(js)


}


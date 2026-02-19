// Package dashboard provides a simple embedded web dashboard
package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
)

// HTML templates
//
//go:embed templates/*.html
var Templates embed.FS

// Handler returns the dashboard HTTP handler
func Handler(clusterInfo interface {
	GetClusterInfo() interface{}
	GetNodes() interface{}
}) http.Handler {
	mux := http.NewServeMux()

	// Serve index
	mux.HandleFunc("/", indexHandler)

	// Serve metrics dashboard
	mux.HandleFunc("/_dashboard/metrics", metricsHandler)

	// Serve cluster dashboard
	mux.HandleFunc("/_dashboard/cluster", clusterHandler(clusterInfo))

	// Serve S3 browser
	mux.HandleFunc("/_dashboard/browser", s3BrowserHandler)

	// API endpoints for real-time data
	mux.HandleFunc("/_dashboard/api/status", apiStatusHandler)
	mux.HandleFunc("/_dashboard/api/metrics", apiMetricsHandler)
	mux.HandleFunc("/_dashboard/api/buckets", apiBucketsHandler)

	return mux
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/index.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint Dashboard",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/metrics.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint Metrics",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}

func clusterHandler(clusterInfo interface {
	GetClusterInfo() interface{}
	GetNodes() interface{}
}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if it's an API request
		if r.URL.Query().Get("format") == "json" || r.Header.Get("Accept") == "application/json" {
			w.Header().Set("Content-Type", "application/json")

			var nodes interface{}
			var replicationFactor int

			if clusterInfo != nil {
				nodes = clusterInfo.GetNodes()
				info := clusterInfo.GetClusterInfo()
				if info != nil {
					if ci, ok := info.(interface{ ReplicationFactor() int }); ok {
						replicationFactor = ci.ReplicationFactor()
					}
				}
			} else {
				nodes = []interface{}{}
			}

			response := map[string]interface{}{
				"replicationFactor": replicationFactor,
				"totalStorage":      int64(0),
				"nodes":            nodes,
			}

			json.NewEncoder(w).Encode(response)
			return
		}

		// Serve HTML
		tmpl, err := template.ParseFS(Templates, "templates/cluster.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		data := struct {
			Title   string
			Version string
		}{
			Title:   "OpenEndpoint Cluster",
			Version: "1.0.0",
		}

		tmpl.Execute(w, data)
	}
}

func s3BrowserHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/browser.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint S3 Browser",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}

// API handlers for real-time data

// getBackendURL returns the backend URL based on the request
func getBackendURL(r *http.Request) string {
	host := r.Host
	if host == "" {
		host = "localhost:9000"
	}
	return "http://" + host
}

func apiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	resp, err := http.Get(backend + "/_mgmt/")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&status)

	status["timestamp"] = map[string]interface{}{
		"seconds": 1728000000,
		"nanos":  0,
	}

	json.NewEncoder(w).Encode(status)
}

func apiMetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	resp, err := http.Get(backend + "/metrics")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	// Parse Prometheus metrics format (simplified)
	// In production, you'd use a proper Prometheus parser
	var metrics struct {
		RequestsTotal     int64 `json:"requests_total"`
		BytesUploaded     int64 `json:"bytes_uploaded"`
		BytesDownloaded   int64 `json:"bytes_downloaded"`
		AvgRequestSeconds int64 `json:"avg_request_seconds"`
	}

	// Parse the metrics response (simplified)
	// Real implementation would parse Prometheus format
	metrics.RequestsTotal = 0
	metrics.BytesUploaded = 0
	metrics.BytesDownloaded = 0

	json.NewEncoder(w).Encode(metrics)
}

func apiBucketsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	resp, err := http.Get(backend + "/_mgmt/buckets")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"buckets": []interface{}{},
			"error":   err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	var buckets map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&buckets)

	// Get object counts for each bucket
	if bucketList, ok := buckets["buckets"].([]interface{}); ok {
		detailedBuckets := make([]map[string]interface{}, 0)
		for _, b := range bucketList {
			if bucket, ok := b.(map[string]interface{}); ok {
				bucketName := bucket["name"].(string)

				// Get object count
				objResp, err := http.Get(backend + fmt.Sprintf("/_mgmt/buckets/%s/objects", bucketName))
				if err == nil {
					var objResult map[string]interface{}
					json.NewDecoder(objResp.Body).Decode(&objResult)
					objResp.Body.Close()
					if contents, ok := objResult["Contents"].([]interface{}); ok {
						bucket["objectCount"] = len(contents)
					}
				}
				detailedBuckets = append(detailedBuckets, bucket)
			}
		}
		buckets["buckets"] = detailedBuckets
	}

	json.NewEncoder(w).Encode(buckets)
}

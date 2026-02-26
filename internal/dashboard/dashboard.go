// Package dashboard provides a simple embedded web dashboard
package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/openendpoint/openendpoint/internal/version"
)

// HTTP client with timeout and connection pooling for backend communication
var backendClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:    90 * time.Second,
	},
}

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
		Version: version.Version,
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
		Version: version.Version,
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
				"nodes":             nodes,
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
			Version: version.Version,
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
		Version: version.Version,
	}

	tmpl.Execute(w, data)
}

// API handlers for real-time data

// getBackendURL returns the backend URL based on the request
func getBackendURL(r *http.Request) string {
	host := r.Host
	if host == "" {
		// Use environment variable or empty string instead of hardcoded localhost
		host = os.Getenv("DEFAULT_BACKEND_HOST")
		if host == "" {
			return "" // Don't fallback to insecure default
		}
	}
	return "http://" + host
}

func apiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	if backend == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": "backend URL not configured",
		})
		return
	}
	resp, err := backendClient.Get(backend + "/_mgmt/")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		status = make(map[string]interface{})
	}
	if status == nil {
		status = make(map[string]interface{})
	}

	status["timestamp"] = map[string]interface{}{
		"seconds": time.Now().Unix(),
		"nanos":   0,
	}

	json.NewEncoder(w).Encode(status)
}

func apiMetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	if backend == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "backend URL not configured",
		})
		return
	}
	resp, err := backendClient.Get(backend + "/_mgmt/metrics/json")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	var metrics map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		// Fallback to empty metrics
		metrics = map[string]interface{}{}
	}

	json.NewEncoder(w).Encode(metrics)
}

func apiBucketsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	backend := getBackendURL(r)
	if backend == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"buckets": []interface{}{},
			"error":   "backend URL not configured",
		})
		return
	}
	resp, err := backendClient.Get(backend + "/_mgmt/buckets")
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
				objResp, err := backendClient.Get(backend + fmt.Sprintf("/_mgmt/buckets/%s/objects", bucketName))
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

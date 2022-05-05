package controllers

import (
	"encoding/json"
	"log"
	"net/http"

	"garm/apiserver/params"
	gErrors "garm/errors"
	runnerParams "garm/params"

	"github.com/gorilla/mux"
)

func (a *APIController) ListAllPoolsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	pools, err := a.r.ListAllPools(ctx)

	if err != nil {
		log.Printf("listing pools: %s", err)
		handleError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pools)
}

func (a *APIController) GetPoolByIDHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	poolID, ok := vars["poolID"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(params.APIErrorResponse{
			Error:   "Bad Request",
			Details: "No pool ID specified",
		})
		return
	}

	pool, err := a.r.GetPoolByID(ctx, poolID)
	if err != nil {
		log.Printf("fetching pool: %s", err)
		handleError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pool)
}

func (a *APIController) DeletePoolByIDHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	poolID, ok := vars["poolID"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(params.APIErrorResponse{
			Error:   "Bad Request",
			Details: "No pool ID specified",
		})
		return
	}

	if err := a.r.DeletePoolByID(ctx, poolID); err != nil {
		log.Printf("removing pool: %s", err)
		handleError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

}

func (a *APIController) UpdatePoolByIDHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	vars := mux.Vars(r)
	poolID, ok := vars["poolID"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(params.APIErrorResponse{
			Error:   "Bad Request",
			Details: "No pool ID specified",
		})
		return
	}

	var poolData runnerParams.UpdatePoolParams
	if err := json.NewDecoder(r.Body).Decode(&poolData); err != nil {
		log.Printf("failed to decode: %s", err)
		handleError(w, gErrors.ErrBadRequest)
		return
	}

	pool, err := a.r.UpdatePoolByID(ctx, poolID, poolData)
	if err != nil {
		log.Printf("fetching pool: %s", err)
		handleError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pool)

}

// Code generated by go-swagger; DO NOT EDIT.

package organizations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
)

// NewListOrgPoolsParams creates a new ListOrgPoolsParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewListOrgPoolsParams() *ListOrgPoolsParams {
	return &ListOrgPoolsParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewListOrgPoolsParamsWithTimeout creates a new ListOrgPoolsParams object
// with the ability to set a timeout on a request.
func NewListOrgPoolsParamsWithTimeout(timeout time.Duration) *ListOrgPoolsParams {
	return &ListOrgPoolsParams{
		timeout: timeout,
	}
}

// NewListOrgPoolsParamsWithContext creates a new ListOrgPoolsParams object
// with the ability to set a context for a request.
func NewListOrgPoolsParamsWithContext(ctx context.Context) *ListOrgPoolsParams {
	return &ListOrgPoolsParams{
		Context: ctx,
	}
}

// NewListOrgPoolsParamsWithHTTPClient creates a new ListOrgPoolsParams object
// with the ability to set a custom HTTPClient for a request.
func NewListOrgPoolsParamsWithHTTPClient(client *http.Client) *ListOrgPoolsParams {
	return &ListOrgPoolsParams{
		HTTPClient: client,
	}
}

/*
ListOrgPoolsParams contains all the parameters to send to the API endpoint

	for the list org pools operation.

	Typically these are written to a http.Request.
*/
type ListOrgPoolsParams struct {

	/* OrgID.

	   Organization ID.
	*/
	OrgID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the list org pools params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ListOrgPoolsParams) WithDefaults() *ListOrgPoolsParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the list org pools params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ListOrgPoolsParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the list org pools params
func (o *ListOrgPoolsParams) WithTimeout(timeout time.Duration) *ListOrgPoolsParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the list org pools params
func (o *ListOrgPoolsParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the list org pools params
func (o *ListOrgPoolsParams) WithContext(ctx context.Context) *ListOrgPoolsParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the list org pools params
func (o *ListOrgPoolsParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the list org pools params
func (o *ListOrgPoolsParams) WithHTTPClient(client *http.Client) *ListOrgPoolsParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the list org pools params
func (o *ListOrgPoolsParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithOrgID adds the orgID to the list org pools params
func (o *ListOrgPoolsParams) WithOrgID(orgID string) *ListOrgPoolsParams {
	o.SetOrgID(orgID)
	return o
}

// SetOrgID adds the orgId to the list org pools params
func (o *ListOrgPoolsParams) SetOrgID(orgID string) {
	o.OrgID = orgID
}

// WriteToRequest writes these params to a swagger request
func (o *ListOrgPoolsParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param orgID
	if err := r.SetPathParam("orgID", o.OrgID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

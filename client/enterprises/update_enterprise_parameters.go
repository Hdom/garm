// Code generated by go-swagger; DO NOT EDIT.

package enterprises

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

	garm_params "github.com/cloudbase/garm/params"
)

// NewUpdateEnterpriseParams creates a new UpdateEnterpriseParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewUpdateEnterpriseParams() *UpdateEnterpriseParams {
	return &UpdateEnterpriseParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewUpdateEnterpriseParamsWithTimeout creates a new UpdateEnterpriseParams object
// with the ability to set a timeout on a request.
func NewUpdateEnterpriseParamsWithTimeout(timeout time.Duration) *UpdateEnterpriseParams {
	return &UpdateEnterpriseParams{
		timeout: timeout,
	}
}

// NewUpdateEnterpriseParamsWithContext creates a new UpdateEnterpriseParams object
// with the ability to set a context for a request.
func NewUpdateEnterpriseParamsWithContext(ctx context.Context) *UpdateEnterpriseParams {
	return &UpdateEnterpriseParams{
		Context: ctx,
	}
}

// NewUpdateEnterpriseParamsWithHTTPClient creates a new UpdateEnterpriseParams object
// with the ability to set a custom HTTPClient for a request.
func NewUpdateEnterpriseParamsWithHTTPClient(client *http.Client) *UpdateEnterpriseParams {
	return &UpdateEnterpriseParams{
		HTTPClient: client,
	}
}

/*
UpdateEnterpriseParams contains all the parameters to send to the API endpoint

	for the update enterprise operation.

	Typically these are written to a http.Request.
*/
type UpdateEnterpriseParams struct {

	/* Body.

	   Parameters used when updating the enterprise.
	*/
	Body garm_params.UpdateEntityParams

	/* EnterpriseID.

	   The ID of the enterprise to update.
	*/
	EnterpriseID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the update enterprise params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *UpdateEnterpriseParams) WithDefaults() *UpdateEnterpriseParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the update enterprise params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *UpdateEnterpriseParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the update enterprise params
func (o *UpdateEnterpriseParams) WithTimeout(timeout time.Duration) *UpdateEnterpriseParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the update enterprise params
func (o *UpdateEnterpriseParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the update enterprise params
func (o *UpdateEnterpriseParams) WithContext(ctx context.Context) *UpdateEnterpriseParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the update enterprise params
func (o *UpdateEnterpriseParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the update enterprise params
func (o *UpdateEnterpriseParams) WithHTTPClient(client *http.Client) *UpdateEnterpriseParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the update enterprise params
func (o *UpdateEnterpriseParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the update enterprise params
func (o *UpdateEnterpriseParams) WithBody(body garm_params.UpdateEntityParams) *UpdateEnterpriseParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the update enterprise params
func (o *UpdateEnterpriseParams) SetBody(body garm_params.UpdateEntityParams) {
	o.Body = body
}

// WithEnterpriseID adds the enterpriseID to the update enterprise params
func (o *UpdateEnterpriseParams) WithEnterpriseID(enterpriseID string) *UpdateEnterpriseParams {
	o.SetEnterpriseID(enterpriseID)
	return o
}

// SetEnterpriseID adds the enterpriseId to the update enterprise params
func (o *UpdateEnterpriseParams) SetEnterpriseID(enterpriseID string) {
	o.EnterpriseID = enterpriseID
}

// WriteToRequest writes these params to a swagger request
func (o *UpdateEnterpriseParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error
	if err := r.SetBodyParam(o.Body); err != nil {
		return err
	}

	// path param enterpriseID
	if err := r.SetPathParam("enterpriseID", o.EnterpriseID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

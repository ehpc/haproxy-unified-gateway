# Gateway API Specification: we must support/unsupport bundle versions


## Rationale
Spec: we MUST maintain 2 status for Gateway Classes:
- type: Accepted
- type: SupportedVersion



Example:
```yaml
status:
  conditions:
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: GatewayClass is accepted
    observedGeneration: 3
    reason: Accepted
    status: "True"
    type: Accepted
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: Gateway API CRD versions are supported
    observedGeneration: 3
    reason: SupportedVersion
    status: "True"
    type: SupportedVersion

```


There 2 implementations possible:
- Best effort = accept unrecgonized/unsupported versions
- Reject unrecgonized/unsupported versions

Question:
- what mode should we implement? best effort ? reject ?
- how many versions to accept ?


## How to migrate ???

- who does install Gateway API CRDs
- is the life cycle the same as our controller (= We install the Gateway API CRDS) = we controller the Gateway API versions ?
- Depending on the our policy: BEST EFFORT/REJECT:
  - how to migrate ?


-------------------------------------------


# How to get Gateway API bundleVersion

Gateway API versions in specified in all Gateway API CRDS:

```yaml
Name:         gatewayclasses.gateway.networking.k8s.io
Namespace:
Labels:       <none>
Annotations:  api-approved.kubernetes.io: https://github.com/kubernetes-sigs/gateway-api/pull/3328
              gateway.networking.k8s.io/bundle-version: v1.2.1
              gateway.networking.k8s.io/channel: standard
API Version:  apiextensions.k8s.io/v1
Kind:         CustomResourceDefinition
```

Annotation:
- gateway.networking.k8s.io/bundle-version: v1.2.1





---------------------------------------



**NOTE:  we will also need to have several controller supporting different versions  of Gateway APIS for different clusters in FUSION**



-----------------------------
# OPTION 1: BEST EFFORT

= Accept unrecgonized/unsupported versions

```yaml
status:
  conditions:
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: GatewayClass is accepted but in best effort mode, supported versions are ~1.2
    observedGeneration: 3
    reason: Accepted
    status: "True"
    type: Accepted
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: Gateway API CRD versions are supported
    observedGeneration: 3
    reason: UnsupportedVersion
    status: "False"
    type: SupportedVersion
```

Customer must have a monitoring tool to watch:
- condition Type SupportedVersion
and monitor that it's a Best Effort versions => upgrade needed of the controller.





# OPTION 2: SUPPORT ONLY 2 MINOR versions (limited list)

- Support only 2 versions: for example ~1.1, ~1.2

```yaml
  conditions:
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: Gateway API CRD versions are not supported. Please install version ~1.1 or ~1.2
    observedGeneration: 3
    reason: Accepted
    ----> status: "False"
    type: Accepted
  - lastTransitionTime: "2025-05-09T07:26:05Z"
    message: Gateway API CRD versions are not supported. Please install version ~1.1 or ~1.2
    observedGeneration: 3
    reason: UnsupportedVersion
    status: "False"
    type: SupportedVersion
```

WARNiNG: setting Accepted = False => Invalidates the whole routing tree behind the Gateway Class
-> OUTAGE


Suppose as today the Gateway API byndle version is v1.2.1 and next vesrion will be v1.3.0
So we must support at least 2 versions and the migration should be:
- 1) Controller supports ~v1.1, ~v.1.2
- 2) Deploy a controller that supports ~v1.2 and ~v1.3
- 3) Then deploy a new Gateway API v1.3.0

If the Gateway API v1.3.0 is deployed before upgrading the controller: OUTAGE



--------------------------------------
--------------------------------------

*from gateway_class_types.go*

BEST EFFORT or not ?

```go
	// This condition indicates whether the GatewayClass supports the version(s)
	// of Gateway API CRDs present in the cluster. This condition MUST be set by
	// a controller when it marks a GatewayClass "Accepted".
	//
	// The version of a Gateway API CRD is defined by the
	// gateway.networking.k8s.io/bundle-version annotation on the CRD. If
	// implementations detect any Gateway API CRDs that either do not have this
	// annotation set, or have it set to a version that is not recognized or
	// supported by the implementation, this condition MUST be set to false.
	//
	// Implementations MAY choose to either provide "best effort" support when
	// an unrecognized CRD version is present. This would be communicated by
	// setting the "Accepted" condition to true and the "SupportedVersion"
	// condition to false.
	//
	// Alternatively, implementations MAY choose not to support CRDs with
	// unrecognized versions. This would be communicated by setting the
	// "Accepted" condition to false with the reason "UnsupportedVersions".
	//
	// Possible reasons for this condition to be true are:
	//
	// * "SupportedVersion"
	//
	// Possible reasons for this condition to be False are:
	//
	// * "UnsupportedVersion"
	//
	// Controllers should prefer to use the values of GatewayClassConditionReason
	// for the corresponding Reason, where appropriate.
	//
	// <gateway:experimental>
	GatewayClassConditionStatusSupportedVersion GatewayClassConditionType = "SupportedVersion"
```

```go
const (
	// This condition indicates whether the GatewayClass has been accepted by
	// the controller requested in the `spec.controller` field.
	//
	// This condition defaults to Unknown, and MUST be set by a controller when
	// it sees a GatewayClass using its controller string. The status of this
	// condition MUST be set to True if the controller will support provisioning
	// Gateways using this class. Otherwise, this status MUST be set to False.
	// If the status is set to False, the controller SHOULD set a Message and
	// Reason as an explanation.
	//
	// Possible reasons for this condition to be true are:
	//
	// * "Accepted"
	//
	// Possible reasons for this condition to be False are:
	//
	// * "InvalidParameters"
	// * "Unsupported"
	// * "UnsupportedVersion"
	//
	// Possible reasons for this condition to be Unknown are:
	//
	// * "Pending"
	//
	// Controllers should prefer to use the values of GatewayClassConditionReason
	// for the corresponding Reason, where appropriate.
	GatewayClassConditionStatusAccepted GatewayClassConditionType = "Accepted"
```
-------------------
# Life cycle Gateway API

https://gateway-api.sigs.k8s.io/concepts/versioning/


## Patch Version (e.g. v0.4.0 -> v0.4.1)
API Spec:
- Clarifications
- Correcting typos

Bug fixes:
- Correcting validation
- Fixes to release process or artifacts

Conformance tests:
- Fixes for existing tests
- Additional conformance test coverage for existing features

## Minor Version (e.g. v0.4.0 -> v0.5.0)

Everything that is valid in a patch release

Experimental Channel:
- Adding new API fields or resources
- Breaking changes for existing API fields or resources
- Removing API fields or resources without prior deprecation

Standard Channel:
- Graduation of fields or resources from Experimental to Standard Channel
- Removal of an API resource following Kubernetes deprecation policy

All Channels:
- Changes to recommended conditions or reasons in status
- Loosened validation (including making required fields optional)
- Changes to conformance tests to match spec updates
- Introduction of a new API version which may include renamed fields or anything else that is valid in a new Kubernetes API version

## Major Version (e.g. v0.x to v1.0)

There are no API compatibility guarantees when the major version changes.

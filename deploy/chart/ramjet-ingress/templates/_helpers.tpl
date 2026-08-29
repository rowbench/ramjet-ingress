{{/*
Name helpers, in the conventional shape: `name` is the chart, `fullname` is the
release-qualified object name, both truncated to the 63 characters a label
value allows.
*/}}
{{- define "ramjet-ingress.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ramjet-ingress.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ramjet-ingress.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ramjet-ingress.labels" -}}
helm.sh/chart: {{ include "ramjet-ingress.chart" . }}
{{ include "ramjet-ingress.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: controller
{{- end -}}

{{/*
Selector labels are the subset that must never change for an existing release:
a Deployment's selector is immutable, so anything varying (the chart version,
the app version) has to stay out of this set.
*/}}
{{- define "ramjet-ingress.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ramjet-ingress.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ramjet-ingress.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ramjet-ingress.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "ramjet-ingress.ingressClassName" -}}
{{- default .Values.controller.ingressClass .Values.ingressClass.name -}}
{{- end -}}

{{/*
The Service whose address gets written into managed Ingresses' status.

Defaulting this to the chart's own LoadBalancer Service is what makes status
writeback work out of the box: the controller reads that Service's
.status.loadBalancer and copies the address onto every Ingress it manages, so
an Ingress in an unrelated namespace ends up advertising the address traffic
really arrives on rather than nothing at all.
*/}}
{{- define "ramjet-ingress.publishService" -}}
{{- if .Values.controller.publishService -}}
{{- .Values.controller.publishService -}}
{{- else -}}
{{- printf "%s/%s" .Release.Namespace (include "ramjet-ingress.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Combinations of values that render valid YAML the API server will happily
accept and that are nonetheless wrong. Each of these was reachable by editing
one value in isolation, and each fails at runtime rather than at install time —
as a pod that will not start, or as a Service port wired to nothing. Failing
the render is the cheaper place to find out.
*/}}
{{- define "ramjet-ingress.validate" -}}
{{- if not (has .Values.kind (list "Deployment" "DaemonSet")) -}}
{{- fail (printf "kind must be Deployment or DaemonSet, got %q" (toString .Values.kind)) -}}
{{- end -}}
{{- if not .Values.ports.https -}}
{{- range $name, $port := dict "http" .Values.service.http "https" .Values.service.https -}}
{{- if eq (toString $port.targetPort) "https" -}}
{{- fail (printf "service.%s.targetPort is \"https\" but ports.https is 0, so no such container port exists — point it at \"http\" (the load balancer is terminating TLS) or give ports.https a port back" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and .Values.http3.enabled (not .Values.ports.https) -}}
{{- fail "http3.enabled needs ports.https: HTTP/3 is served on that port number in UDP, and the alt-svc header that advertises it rides on that listener's responses. The daemon refuses the combination at startup, so the pod would not start" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled .Values.hostNetwork -}}
{{- fail "networkPolicy.enabled with hostNetwork: true renders a NetworkPolicy that does not restrict the admin port. A host-network pod is in the node's network namespace and matches no podSelector, so no CNI enforces the policy on it — and the admin listener is bound on the node's own interfaces there rather than on a pod IP, so it is reachable from anything that can route to the node. An object that reads as a restriction and enforces nothing is worse than no object. Use controller.adminToken, which works in this shape; add controller.extraArgs: [--admin=127.0.0.1:10254] if nothing off-node scrapes it; or set hostNetwork: false" -}}
{{- end -}}
{{/*
Deliberately not validated here: proxyProtocol.enabled without a matching
provider annotation. It looks like the same class of mistake, but an external
HAProxy or MetalLB-fronted edge is a perfectly good reason to expect the header
with no Service annotation anywhere — and a guard that refuses a working
configuration is worse than the documentation that explains the pairing.
*/}}
{{- end -}}

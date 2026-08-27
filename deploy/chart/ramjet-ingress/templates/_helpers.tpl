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

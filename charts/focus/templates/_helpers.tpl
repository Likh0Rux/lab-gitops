{{/* Label comuni a tutte le risorse del chart */}}
{{- define "focus.labels" -}}
app.kubernetes.io/part-of: focus
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* securityContext comune: non-root, niente escalation, nessuna capability */}}
{{- define "focus.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
capabilities:
  drop: [ALL]
{{- end -}}

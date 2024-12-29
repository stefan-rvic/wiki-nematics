FROM grafana/grafana:latest

COPY ./provisioning /etc/grafana/provisioning
COPY ./dashboards /var/lib/grafana/dashboards
COPY ./mongodb-grafana /var/lib/grafana/plugins/mongodb-grafana

ENV GF_SERVER_HTTP_PORT=8080
EXPOSE 8080

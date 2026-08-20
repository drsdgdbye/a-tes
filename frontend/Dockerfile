FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf

COPY index.html /usr/share/nginx/html/index.html
COPY tasks.html /usr/share/nginx/html/tasks.html
COPY accounting.html /usr/share/nginx/html/accounting.html
COPY analytics.html /usr/share/nginx/html/analytics.html
COPY css/ /usr/share/nginx/html/css/
COPY js/ /usr/share/nginx/html/js/

EXPOSE 80

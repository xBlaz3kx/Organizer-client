# Pull python Image
FROM python:3.7
RUN apt-get update && apt-get install -y \
    curl \
    git

# Copy all client files
WORKDIR /etc/StockUp/Organizer/Python-client
COPY . .

# Install libraries in requirements.txt
RUN sudo pip3 install -r requirements.txt

CMD ["sudo python3","main.py"]

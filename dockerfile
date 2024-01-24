# set base image (host OS)
FROM code.vt.edu:5005/aaron2000/team-2-central/base_search_image:1.1

# copy the content of the local src directory to the working directory
COPY . .

# expose port
EXPOSE 5000

CMD python ./app.py

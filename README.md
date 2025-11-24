# STADVDB_MCO2
 (sorry I'll fix the formatting of this later)

 Run the index file on a node server
 1. Log in to the proxmox VM using Cloud Login Credentials (from the emailed excel), change the dropdown to linux
 2. Navigate to one of the node servers, go to the console
 3. check if git, flask, and python are installed (git --version, python3 --version, pip3 show flask)
 4. clone the git repo 
    git clone https://github.com/Immern/STADVDB_MCO2
 5. cd STADVDB_MCO2/
 6. make sure ur in the repo folder, "ls"
 7. run app.py
    python3 app.py
 8. go to chrome, type in "http://ccscloud.dlsu.edu.ph:<external node port>"
    external node port = In the port forwarding column of the excel, each node server has 3 rows, copy the external port in the "HTTP from-" section
 9. The index file should apppear

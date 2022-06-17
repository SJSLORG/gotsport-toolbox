# Arbiter Import - Fall 2021 - GotSport

https://github.com/SJSLORG/gotsport-toolbox

Results are here (I attached a copy for review): etl/data/export/ArbiterAnalysis.Fall2021.v3.xlsx

## Just the facts

| Total Category | Expected | Actual |
|----------------|----------|--------|			
| Total Exported from GotSport | X	||
| Total Import Actual |	|X |
| Total Import Errors | |X |
| TBR (7/4) | | X |
| Missing | | X |


## Gory details

Script: https://github.com/SJSLORG/gotsport-toolbox/blob/main/etl/arbiterimport.py

1. Get game export from GotSport (go to Schedules > Event > Spring 2022 > Match List and Export from right side menu).
2. Validate Venues and Pitches are correct in the ArbiterMapping.xslx file: etl/data/lookup/ArbiterMappings.xlsx
3. Confirm paths in python script match your export and ArbiterMapping file.
4. Run the python script to create a raw export of games with Arbiter columns and data matched.
4. Using the Game Template from Arbiter Schedule import, paste the game data to that worksheet.  Check validations.
5. Perform import on Arbiter screen.  Download Error file for review.
6. Rinse and Repeat.
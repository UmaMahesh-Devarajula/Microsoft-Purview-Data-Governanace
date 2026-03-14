import sys
from datasources.registerdatasource import registerdatasource
from datasources.deletedatasource import deletedatasource
from datasources.listdatasources import listdatasources
from datasources.exportdatasources import exportdatasources
from datasources.restoredatasources import restoredatasources


def dataSources():    
        
    while True:
        print("""
        1. Register a Data Source
        2. Delete a Data Sources
        3. List Data Sources
        4. Export Data Sources
        5. Restore Data Sources
        6. Go To data Map
        7. Go To purview
        8. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
          registerdatasource()
        elif choice == "2":
          deletedatasource()
        elif choice == "3":
          listdatasources()
        elif choice == "4":
          exportdatasources()
        elif choice == "5":
          restoredatasources()
        elif choice == "6":
          from DataMap.dataMap import datamap
          datamap()
        elif choice == "7":
          import purview# Import inside the function
          purview.purview()
        elif choice == "8":
          print("Exiting... Goodbye!")
          sys.exit(0)
        else:
          print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    dataSources()

import sys
from CreatePurview.createpurview import createpurview
from DataMap.dataMap import datamap

def purview():
    print("*******welcome*******")
    
    while True:
        print("Please choose any of the below options:")
        print("1. Create Purview Account")
        print("2. Access DataMap")
        print("3. Access UnifiedCatalog")
        print("4. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            createpurview()
        elif choice == "2":
            datamap()
        elif choice == "3":
            openUnifiedCatalog()
        elif choice == "4":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")

if __name__ == "__main__":
    purview()
import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

# ADD SECTION
config_file.add_section("APISettings")
# ADD SETTINGS TO SECTION
config_file.set("APISettings", "url", "https://api.schiphol.nl/public-flights/flights")
config_file.set("APISettings", "app_id", "62df7cc4")
config_file.set("APISettings", "app_key", "b115486d9bdb69fff16a65524eb9d474")

# SAVE CONFIG FILE
with open(r"configurations.ini", 'w') as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'configurations.ini' created")

# PRINT FILE CONTENT
read_file = open("configurations.ini", "r")
content = read_file.read()
print("Content of the config file are:\n")
print(content)
read_file.flush()
read_file.close()
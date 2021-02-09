from cryptography.fernet import Fernet

key_file = "D:/Python/Projects/api_extract/config_files/fernet_key.txt"

with open(key_file, "r") as file:
    key = file.read()

f = Fernet(key)
token = f.encrypt(b"password here")
print(token)

from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
 
f = open("./_", "r") 
code = f.readline()
#code = 'blasphemy'
 
with open('./encrypted_data.bin', 'rb') as fobj:
    private_key = RSA.import_key(
        open('./my_private_rsa_key.pem').read(),
        passphrase=code)
 
    enc_session_key, nonce, tag, ciphertext = [ fobj.read(x) 
                                                for x in (private_key.size_in_bytes(), 
                                                16, 16, -1) ]
 
    cipher_rsa = PKCS1_OAEP.new(private_key)
    session_key = cipher_rsa.decrypt(enc_session_key)
 
    cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce)
    data = cipher_aes.decrypt_and_verify(ciphertext, tag)
    
data = data.decode('utf-8').replace('\n', '|') 

#print(data)

p_list = data.split("|")

p_aws_access_key_id = p_list[0]
p_aws_secret_access_key = p_list[1]
p_aws_s3_bucket_name = p_list[2]
#p_edp_object_name = p_list[3]
p_snowflake_user = p_list[3]
p_snowflake_password = p_list[4]
p_snowflake_account = p_list[5]
p_snowflake_database = p_list[6]
p_snowflake_warehouse = p_list[7]
p_snowflake_schema = p_list[8]
p_hostname = p_list[9]
p_username = p_list[10]
p_password = p_list[11]
p_database1 = p_list[12]
p_port1 = p_list[13]
p_sftp_server = p_list[14]
p_sftp_username = p_list[15]
p_sftp_password = p_list[16]


"""
print("p_aws_access_key_id = ", p_list[0])
print("p_aws_secret_access_key = ", p_list[1])
print("p_aws_s3_bucket_name = ", p_list[2])
print("p_edp_object_name = ", p_list[3])
print("p_snowflake_user = ", p_list[3])
print("p_snowflake_password = ", p_list[4])
print("p_snowflake_account = ", p_list[5])
print("p_snowflake_database = ", p_list[6])
print("p_snowflake_warehouse = ", p_list[7])
print("p_snowflake_schema = ", p_list[8])
print("p_hostname = ", p_list[9])
print("p_username = ", p_list[10])
print("p_password = ", p_list[11])
print("p_datanase1 = ", p_list[12])
print("p_port1 = ", p_list[13])
print("p_sftp_server =  ", p_list[14])
print("p_sftp_username =  ", p_list[15])
print("p_sftp_password =  ", p_list[16])
"""



f.close
fobj.close





#data = data.replace("|", "\n")
#print(data)

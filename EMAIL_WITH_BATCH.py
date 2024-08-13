import pandas as pd
import re
import smtplib
import dns.resolver
import socket
import concurrent.futures
import time
import random



# Configuration
input_filename = 'email_check.xlsx'
sheet_name = 'ss'
output_filename = 'valid_mails.txt'
max_workers = 10  # Number of concurrent workers
timeout = 5  # Timeout for DNS and SMTP connections in seconds
retry_attempts = 5  # Number of retry attempts for SMTP connections

# Regular expressions and SMTP setup
regex = '^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,})$'
fromAddress = 'parasyadav0502@gmail.com'

def check_email(addressToVerify):
    def attempt_connection(mxRecord, retries):
        for attempt in range(retries):
            try:
                with smtplib.SMTP(mxRecord, timeout=timeout) as server:
                    server.set_debuglevel(0)
                    server.helo()
                    server.mail(fromAddress)
                    code, _ = server.rcpt(addressToVerify)
                return code == 250
            except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected, smtplib.SMTPRecipientsRefused, socket.timeout) as e:
                print(f"SMTP attempt {attempt + 1} failed for {addressToVerify}: {e}")
                time.sleep(random.uniform(1, 3))  # Random delay to avoid rapid retries
        return False

    try:
        # Check syntax
        if not re.match(regex, addressToVerify):
            return None

        # Get domain for DNS lookup
        domain = addressToVerify.split('@')[1]

        # DNS MX record lookup
        resolver = dns.resolver.Resolver()
        resolver.timeout = timeout
        resolver.lifetime = timeout

        try:
            records = resolver.resolve(domain, 'MX')
            mxRecord = str(records[0].exchange)
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout) as e:
            print(f"DNS resolution failed for domain {domain}: {e}")
            return None

        # SMTP verification with retry logic
        is_valid = attempt_connection(mxRecord, retry_attempts)
        if is_valid:
            return addressToVerify
        else:
            print(f"SMTP verification failed for {addressToVerify}")
            return None

    except Exception as e:
        print(f"Unexpected error checking {addressToVerify}: {e}")
        return None

def process_chunk(chunk):
    valid_emails = []
    for email in chunk:
        valid_email = check_email(email)
        if valid_email:
            valid_emails.append(valid_email)
    return valid_emails

def save_valid_emails(valid_emails):
    with open(output_filename, 'a') as file:
        for email in valid_emails:
            file.write(f"{email}\n")

def split_list(lst, num_chunks):
    avg_chunk_size = len(lst) // num_chunks
    chunks = [lst[i:i + avg_chunk_size] for i in range(0, len(lst), avg_chunk_size)]
    return chunks

def excel_to_flat_list(filename, sheet_name):
    df = pd.read_excel(filename, sheet_name=sheet_name, header=None)
    data_list = df.values.flatten().tolist()
    data_list = [x for x in data_list if pd.notnull(x)]
    return data_list

# Read Excel file
data_list = excel_to_flat_list(input_filename, sheet_name)

# Split emails into chunks
email_chunks = split_list(data_list, max_workers)

# Process each chunk concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(process_chunk, chunk) for chunk in email_chunks]
    for future in concurrent.futures.as_completed(futures):
        valid_emails = future.result()
        if valid_emails:
            save_valid_emails(valid_emails)

print(f"Processing complete. Valid emails are written to {output_filename}.")

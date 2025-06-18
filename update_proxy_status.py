import requests
import csv
import shutil
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import ipaddress  # Untuk mengurutkan IP

def bersihkan_isp(isp):
    """Membersihkan teks ISP dari karakter titik dan koma menjadi kosong"""
    return isp.replace('.', '').replace(',', '').strip()

def ip_sort_key(ip_str):
    """Fungsi untuk mengurutkan alamat IP secara numerik"""
    try:
        ip = ipaddress.ip_address(ip_str)
        return (0, ip)  # IPv4/IPv6 valid
    except ValueError:
        return (1, ip_str)  # IP tidak valid

def check_proxy_single(ip, port, api_url_template):
    """Mengecek satu proxy menggunakan API."""
    try:
        # Format URL API untuk satu proxy
        api_url = api_url_template.format(ip=ip, port=port)
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Ambil status proxyip
        status = data[0].get("proxyip", False)
        if status:
            print(f"{ip}:{port} is ALIVE")
            return (ip, port, None)  # Format: (ip, port, None)
        else:
            print(f"{ip}:{port} is DEAD")
            return (None, None, f"{ip}:{port} is DEAD")  # Format: (None, None, error_message)
    except requests.exceptions.RequestException as e:
        error_message = f"Error checking {ip}:{port}: {e}"
        print(error_message)
        return (None, None, error_message)
    except ValueError as ve:
        error_message = f"Error parsing JSON for {ip}:{port}: {ve}"
        print(error_message)
        return (None, None, error_message)


def generate_grouped_json(proxy_data, output_file='alive_proxies_grouped.json'):
    """
    Mengelompokkan proxy hidup berdasarkan CC dan ISP,
    lalu memberikan singkatan alfabet a-z untuk setiap ISP dalam tiap CC.
    """
    grouped = {}

    # Kelompokkan berdasarkan cc dan isp
    for row in proxy_data:
        ip, port, cc, isp = row
        # Bersihkan ISP untuk JSON juga
        isp_clean = bersihkan_isp(isp)
        if cc not in grouped:
            grouped[cc] = {}
        if isp_clean not in grouped[cc]:
            grouped[cc][isp_clean] = []
        grouped[cc][isp_clean].append(f"{ip}:{port}")

    # Beri abjad a-z per grup isp dalam tiap cc
    final_structure = {}
    for cc in sorted(grouped.keys()):  # Urutkan berdasarkan countryCode
        final_structure[cc] = {}
        isps = sorted(grouped[cc].keys())  # urutkan ISP
        for idx, isp in enumerate(isps):
            letter = chr(97 + idx)  # a, b, c...
            final_structure[cc][letter] = {
                "name": isp,
                "proxies": grouped[cc][isp]
            }

    # Simpan ke file JSON
    try:
        with open(output_file, 'w') as f:
            json.dump(final_structure, f, indent=2)
        print(f"File JSON berhasil dibuat: {output_file}")
    except Exception as e:
        print(f"Error saat menyimpan file JSON: {e}")


def main():
    input_file = os.getenv('IP_FILE', 'f74bjd2h2ko99f3j5')
    output_file = 'f74bjd2h2ko99f3j5.tmp'
    error_file = 'error.txt'
    api_url_template = os.getenv('API_URL', 'https://proxyip-check.vercel.app/{ip}:{port}')

    alive_proxies = []  # Menyimpan proxy yang aktif dengan format [ip, port, cc, isp]
    error_logs = []  # Menyimpan pesan error
    seen_proxies = set()  # Untuk deteksi duplikat (ip, port)

    try:
        with open(input_file, "r") as f:
            reader = csv.reader(f)
            rows = []
            for row in reader:
                if len(row) >= 4:
                    # Bersihkan kolom ISP
                    row[3] = bersihkan_isp(row[3])
                    rows.append(row)
        print(f"Memproses {len(rows)} baris dari file input.")
    except FileNotFoundError:
        print(f"File {input_file} tidak ditemukan.")
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in rows:
            if len(row) >= 4:
                ip, port = row[0].strip(), row[1].strip()
                
                # Cek duplikat IP:PORT
                proxy_id = f"{ip}:{port}"
                if proxy_id in seen_proxies:
                    print(f"Duplikat ditemukan dan dilewati: {ip}:{port}")
                    continue
                    
                seen_proxies.add(proxy_id)
                futures.append(executor.submit(check_proxy_single, ip, port, api_url_template))

        for future in as_completed(futures):
            ip, port, error = future.result()
            if ip and port:
                # Cari baris yang sesuai dari file input
                for row in rows:
                    if row[0].strip() == ip and row[1].strip() == port:
                        alive_proxies.append(row)  # Simpan seluruh baris (ip, port, cc, isp)
                        break
            if error:
                error_logs.append(error)

    # Urutkan berdasarkan IP
    alive_proxies.sort(key=lambda row: ip_sort_key(row[0].strip()))

    # Tulis proxy yang aktif ke file output sementara
    try:
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(alive_proxies)
        print(f"File output {output_file} telah diperbarui.")
    except Exception as e:
        print(f"Error menulis ke {output_file}: {e}")
        return

    # Tulis error ke file error.txt
    if error_logs:
        try:
            with open(error_file, "w") as f:
                for error in error_logs:
                    f.write(error + "\n")
            print(f"Beberapa error telah dicatat di {error_file}.")
        except Exception as e:
            print(f"Error menulis ke {error_file}: {e}")
            return

    # Ganti file input dengan file output
    try:
        shutil.move(output_file, input_file)
        print(f"{input_file} telah diperbarui dengan proxy yang ALIVE.")
    except Exception as e:
        print(f"Error menggantikan {input_file}: {e}")

    # Buat file JSON berdasarkan kelompok cc+isp
    generate_grouped_json(alive_proxies)


if __name__ == "__main__":
    main()

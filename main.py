import pandas as pd
import tabula
import os
import sys


def convertTable(df):
    # Hapus Header dan Judul Halaman
    # Halaman Pertama
    if df.iloc[1][0] != "No.":
        abcd = df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                       ).reset_index(drop=True).copy()
    # Bukan Halaman Pertama
    else:
        abcd = df.drop([0, 1, 2]).reset_index(drop=True).copy()

    # Total row sebelum diolah
    real_length = abcd.shape[0]

    # Ambil semua index row NaN
    nanin = abcd.loc[pd.isna(abcd["Nomor"]), :].index
    nanin = nanin.values.tolist()

    # Ambil semua index row non NaN
    active = []
    for i in range(real_length):
        if i not in nanin:
            active.append(i)

    # Isi semua NaN dengan string kosong
    abcd = abcd.fillna("")

    # Kolom yang perlu ditambah spasi untuk baris baru
    spasi = [3, 6, 10]

    # Kasus khusus penambahan spasi bulan Mei
    for a in abcd.iloc[0:]["Tanggal"].tolist():
        if "May" in a:
            spasi.insert(0, 2)
            break

    # Penggabungan row pertama
    # jika "No." kosong
    if not active or active[0] != 0:
        # Salin isi konten pada row pertama
        content = abcd.iloc[0].copy()
        x = 1
        # Jika kolom "No." di row bawahnya kosong
        while x in nanin:
            # Gabungkan data di tiap kolom
            for i in range(11):
                y = abcd.iloc[x][i]
                # penambahan spasi di kolom tertentu
                if i in spasi:
                    if y != "":
                        content[i] = content[i] + " " + y
                else:
                    content[i] += y
            x += 1
        # penambahan row baru dengan data yang telah digabung
        abcd = abcd.append(content, ignore_index=True)

    # Gabung semua row "No." kosong ke row yang memiliki "No."
    # Loop ke row non NaN
    for a in range(len(active)):
        # Salin isi konten pada row non NaN
        content = abcd.iloc[active[a]].copy()
        x = active[a] + 1
        # Jika kolom "No." di row bawahnya kosong
        while x in nanin:
            # Gabungkan data di tiap kolom
            for i in range(11):
                y = abcd.iloc[x][i]
                # penambahan spasi di kolom tertentu
                if i in spasi:
                    if y != "":
                        content[i] = content[i] + " " + y
                else:
                    content[i] += y
            x += 1
        # penambahan row baru dengan data yang telah digabung
        abcd = abcd.append(content, ignore_index=True)

    # Drop data asli
    abcd = abcd.drop(abcd.index[0:real_length]).reset_index(drop=True)
    return abcd


def mergeTable(df):
    merged = pd.DataFrame()
    for i in range(len(df)):
        merged = merged.append(convertTable(df[i]))
    merged = merged.reset_index(drop=True)
    d_index = merged.index[merged["Nomor"] == ""].tolist()
    spasi = [3, 6, 10]
    if "May" in merged.iloc[0][2]:
        spasi.insert(0, 2)

    for d in d_index:
        content = merged.iloc[d - 1]
        for i in range(11):
            y = merged.iloc[d][i]
            if i in spasi:
                if y != "":
                    content[i] = content[i] + " " + y
            else:
                content[i] += y

    merged = merged.drop(merged.index[d_index])

    return merged.reset_index(drop=True)


def ToCSV(path):
    df = pd.DataFrame()
    df = tabula.read_pdf(
        path,
        guess=False,
        pages="all",
        columns=[35.3, 77.4, 135.5, 255.3, 308.6,
                 346.8, 412.8, 545.6, 603.3, 661.9],
        pandas_options={"header": None},
    )

    # print(file[i]+" loaded")
    # print(path+" loaded")

    for data in df:
        data.columns = [
            "No.",
            "Nomor",
            "Tanggal",
            "Jenis Barang",
            "Jml Barang",
            "Satuan",
            "Jns Pengguna",
            "Nama Pengguna",
            "Jns Identitas",
            "No Identitas",
            "Alamat",
        ]

    merged = mergeTable(df).set_index("No.")
    # print(file[i][:-4]+" dataframes merged")
    # print(path[:-4]+" dataframes merged")

    # csv_name = loc + folder + "/csv/" + file[i][:-4] + ".csv"
    csv_name = "csv/" + path[8:-4] + ".csv"
    merged.to_csv(csv_name)
    # print(file[i][:-4]+".csv"+" saved\n")
    print(path[8:])
    # print(csv_name)


path = sys.argv[1]
ToCSV(path)

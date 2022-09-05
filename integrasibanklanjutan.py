import pymysql
import time
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

folder_bank = '1ise4XN7ZAt3v7QHVMwlMGfVKmB0xI5V_'
bank_done = '1XE33h3r655Ub36cjdQxvlKnWQrvPPaGQ'
folder_toko = '14uJIAx-nyxS59LLeZjyoqPYBVN2a9SB6'
toko_done = '1jpgeFjQUv7fTczPbsw3GUe7pJIt-Ieee'

first_boot = 1

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

def connect_db_toko():
    conn = pymysql.connect(host='remotemysql.com', user='qH4xaikDfx', password='dOhkDO4T7h', db='qH4xaikDfx')
    return conn

def connect_db_bank():
    conn = pymysql.connect(host='remotemysql.com', user='IzvgfOVDyn', password='9Ab9vkzPAt', db='IzvgfOVDyn')
    return conn

def update(table, val, cursor, db):
    try:
        sql = "UPDATE " + table + " SET user_id = '%s', id_produk = '%s', jumlah = '%s' , total = '%s' , status = '%s' WHERE id_invoice = %s"
        cursor.execute(sql, val)
        db.commit()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

    return 1

def insert(table, val, cursor, db):
    sql = "INSERT INTO " + table + "(id_invoice, user_id,id_produk,jumlah,total,date,status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, val)
    db.commit()

    return 1

def delete(table, val, cursor, db):
    sql = "DELETE FROM " + table + " WHERE id_invoice = %s"
    cursor.execute(sql, val)
    db.commit()

    return 1

tables_1 = ("tb_invoice","")
histories_1 = ("tb_integrasi_invoice","")

delay_engine = int(input('Masukkan Lama Delay Engine : '))

def fileOperation(table, data, filename, operation, gauth):
    try:
        print("-- PROCESS %s --" % filename)

        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)
        try:
            filepath = './backuptoko/' + filename
            with open(filepath, 'r') as f:
                try:
                    datajson = json.load(f)
                except:
                    datajson = {}
                    datajson[table] = []
        except:
            datajson = {}
            datajson[table] = []

        if(operation != "delete"):
            datajson[table].append({
                'operation': operation,
                'id_invoice': str(data[0]),
                'user_id': str(data[1]),
                'id_produk': str(data[2]),
                'jumlah': str(data[3]),
                'total': str(data[4]),
                'date': str(data[5]),
                'status': str(data[6])
            })
        else:
            datajson[table].append({
                'operation': operation,
                'id_invoice': str(data[0])
            })
        with open(filepath, 'w') as outfile:
            json.dump(datajson, outfile)

        file_list = drive.ListFile({'q': "'%s' in parents" % folder_toko}).GetList()
        try:
            for file1 in file_list:
                if file1['title'] == filename:
                    file1.Delete()
        except:
            pass

        print("-- UPDATE %s --" % filename)
        file_u = drive.CreateFile({'title': '%s' % filename, 'parents':[{"kind": "drive#fileLink", "id": folder_toko}]})
        file_u.SetContentString(json.dumps(datajson))
        file_u.Upload()

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)
    return 1


while (1):
    first_boot = 1
    try:
        connection_to_toko = 1
        try:
            connToko = connect_db_toko()
            curToko = connToko.cursor()
            if (connToko):
                print("DB Toko Terkoneksi")
        except:
            print("Tidak bisa tersambung ke toko")
            connection_to_toko = 0

        try:
            connBank = connect_db_bank()
            curBank = connBank.cursor()
            if (connBank):
                print("DB Bank Terkoneksi")
        except:
            print("Tidak bisa tersambung ke bank")

        #read data dari json history toko saat first boot while (first_boot):
        while (first_boot):
            try:
                file_list = drive.ListFile({'q': "'%s' in parents" % folder_bank}).GetList()
                try:
                    file_list.reverse()
                    for file1 in file_list:
                        if "bank_" in file1['title']:
                            file1.GetContentFile(file1['title'])
                            file1.Delete()
                            with open(file1['title'], 'r') as f:
                                json_dict = json.load(f)
                                print('-- LOADING JSON FILE --')
                            for jsonData in json_dict['tb_integrasi_invoice']:
                                if (jsonData['operation'] !='delete'):
                                    data = []
                                    data.append(jsonData['id_invoice'])
                                    data.append(jsonData['user_id'])
                                    data.append(jsonData['id_produk'])
                                    data.append(jsonData['jumlah'])
                                    data.append(jsonData['total'])
                                    data.append(jsonData['date'])
                                    data.append(jsonData['status'])

                                    if (jsonData['operation'] =='insert'):
                                        val = (data[0], data[1],data[2], data[3], data[4], data[5], data[6])

                                        print('- Insert Data Dari File JSON - id_invoice = %s' % jsonData['id_invoice'])
                                        insert(histories_1[0],val,curBank,connBank)
                                        insert(tables_1[0],val,curBank,connBank)

                                    if (jsonData['operation'] =='update'):
                                        val = (data[1],data[2], data[3], data[4], data[6], data[0])

                                        print('- Update Data Dari File JSON - id_invoice = %s' % jsonData['id_invoice'])
                                        update(histories_1[0],val,curBank,connBank)
                                        update(tables_1[0],val,curBank,connBank)
                                else:
                                    data = []
                                    data.append(jsonData['id_invoice'])
                                    val = (data[0])
                                        
                                    print('- Delete Data Dari File JSON - %s' % jsonData['id_invoice'])
                                    delete(histories_1[0],val,curBank,connBank)
                                    delete(tables_1[0],val,curBank,connBank)

                            folderName = 'bankdone'
                            folders = drive.ListFile({'q': "title='" + folderName + "' and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList()
                            for folder in folders:
                                if folder['title'] == folderName:
                                    filename2 = file1['title']
                                    file2 = drive.CreateFile({'parents':[{"kind": "drive#fileLink", "id":bank_done}]})
                                    file2.SetContentFile(filename2)
                                    file2.Upload()
                                    print('-- DONE LOADING JSON FILE --')
                except:
                    pass
            except:
                print('-- TIDAK TERDAPAT INTEGRASI --')
            first_boot = 0

        sql_select = "SELECT * FROM tb_invoice"
        curBank.execute(sql_select)
        result = curBank.fetchall()

        sql_select = "SELECT * FROM tb_integrasi_invoice"
        curBank.execute(sql_select)
        integrasi = curBank.fetchall()

        # insert listener
        if (len(result) > len(integrasi)):
            print("-- INSERT DETECTED --")
            for data in result:
                a = 0
                for dataIntegrasi in integrasi:
                    if (data[0] == dataIntegrasi[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN INSERT FOR ID = %s" % (data[0]))
                    val = (data[0], data[1], data[2], data[3], data[4], data[5], data[6])
                    insert(histories_1[0],val,curBank,connBank)
                    if (connection_to_toko == 1):
                        timestr = time.strftime("%Y%m%d-%H%M%S")
                        backupfile = 'toko_' + timestr + '.json'
                        fileOperation("tb_integrasi_invoice", data,backupfile, 'insert', gauth)
                    else:
                        timestr = time.strftime("%Y%m%d-%H%M%S")
                        backupfile = 'toko_' + timestr + '.json'
                        fileOperation("tb_integrasi_invoice", data,backupfile, 'insert', gauth)

        # delete listener
        if (len(result) < len(integrasi)):
            print("-- DELETE DETECTED --")
            for dataIntegrasi in integrasi:
                a = 0
                for data in result:
                    if (dataIntegrasi[0] == data[0]):
                        a = 1
                if (a == 0):
                    print("-- RUN DELETE FOR ID = %s" %(dataIntegrasi[0]))
                    delete(histories_1[0],dataIntegrasi[0],curBank,connBank)

                    if (connection_to_toko == 1):
                        timestr = time.strftime("%Y%m%d-%H%M%S")
                        backupfile = 'toko_' + timestr + '.json'
                        fileOperation("tb_integrasi_invoice",dataIntegrasi, backupfile, 'delete', gauth)
                    else:
                        timestr = time.strftime("%Y%m%d-%H%M%S")
                        backupfile = 'toko_' + timestr + '.json'
                        fileOperation("tb_integrasi_invoice",dataIntegrasi, backupfile, 'delete', gauth)

        # update listener
        if (result != integrasi):
            print("-- EVENT SUCCESS OR UPDATE DETECTED --")
            for data in result:
                for dataIntegrasi in integrasi:
                    if (data[0] == dataIntegrasi[0]):
                        if (data != dataIntegrasi):
                            val = (data[1], data[2], data[3], data[4], data [6] , data[0])
                            update(histories_1[0],val,curBank,connBank)
                            if (connection_to_toko == 1):
                                timestr=time.strftime ("%Y%m%d-%H%M%S")
                                backupfile = 'toko_' + timestr +'.json'
                                fileOperation ("tb_integrasi_invoice", data,backupfile, 'update', gauth)
                            else:
                                timestr = time.strftime ("%Y%m%d-%H%M%S")
                                backupfile = 'toko_' + timestr +'.json'
                                fileOperation ("tb_integrasi_invoice", data,backupfile, 'update', gauth)

    except (pymysql.Error, pymysql.Warning) as e:
        print(e)
    # Untuk delay
    time.sleep(delay_engine)

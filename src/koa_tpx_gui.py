
def tpx_gui(table, api_instance):

    #koatpx database SQL query/header
    if table == 'koatpx':
        results = api_instance.searchKOATPX()

        #column new header
        page_header = ['UT Date of Observation', 'Tel Instr', 'PI',
                       '# Original Files', '# Archived Files', 'Science Files',
                       'Size(GB)', 'Summit Disk (sdata#)', 'Data On Stage Disk',
                       'Files Archive-Ready', 'Metadata Sent', '2 DVDs Written',
                       'DVD Stored @ CARA', 'Data Sent to NExScI',
                       'TPX Complete', 'L1 Done', 'L1 Sent to NExScI',
                       'L1 Complete']

    #koadrp database SQL query/header
    else:
        results = api_instance.searchKOADRP()

        #column header
        page_header = ['UT Date of Observation', 'Instrument',' Phase', 'Files',
                       'Reduced','Start Time', 'Start Reduce', 'End Time',
                       'Time Lost','Notes']

    if not results:
        return results, page_header

    if table == 'koatpx':
        for row in results:
            for k, v in row.items():
                #if value is None or N/A, do not populate cell
                if (v is None) or (v == "N/A"):
                    row[k] = ""

            #add line break for Level 1 Done
            if row["lev1_done"]:
                row["lev1_done"] = 'DONE<br>'+row["lev1_done"].strftime("%Y%m%d %H:%M")

            #add line break for DRP Sent
            if row["drpSent"]:
                row["drpSent"] = 'DONE<br>'+row["drpSent"]

            #if file size exists, convert MB -> GB, round to 1/100ths place
            if row["size"]:
                row["size"] /= 1000.
                row["size"] = round(row["size"], 2)

            #if On Disk Time exists
            if row["ondisk_time"]:
                #but On Disk Status does not
                if row["ondisk_stat"] != 'DONE':
                    #set On Disk Status to done
                   row["ondisk_stat"] = 'DONE'

           #if Archive Time exists
            if row["arch_time"]:
                #but Archive Status does not
                if row["arch_stat"] != 'DONE':
                    #set Archive Status to done
                    row["arch_stat"] = 'DONE'

    #if koadrp database
    else:
        #for each dictionary
        for d in results:
            #for key and value in dictionary item
            for k, v in d.items():
                #if value is None or N/A, do not populate cell
                if (v is None) or (v == "N/A"):
                    row[k] = ""

    return results, page_header


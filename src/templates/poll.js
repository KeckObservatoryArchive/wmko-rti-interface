/**
 * Simple long polling client based on JQuery
 */

/**
 * Request an update to the server and once it has answered, then update
 * the content and request again.
 * The server is supposed to response when a change has been made on data.
 */

function update() {
    $.ajax({
        url: '/koarti/data-update',
        success:  function(data) {
            write_table(data);
            update();
        },
        timeout: 5000000 //If timeout is reached run again
    });
}

/**
 * Perform first data request. After taking this data, just query the
 * server and refresh when answered (via update call).
 */
function load() {
    $.ajax({
        url: '/koarti/data',
        success: function(data) {
            write_table(data);
            update();

        }
    });
}

/**
 * Write the table,  only update rows that do not already exist.
 * @param data (list:JSON) a list of JSON objects for each row in the table
 */
function write_table(data) {
    let results = JSON.parse(data.results);
    let column = data.columns;
    let num_results = 0;
    var $table = $('#table-body');

    if (Boolean(results)) {
        num_results = results.length;
        $table.empty();
    }

    for (let i = 0; i < num_results; i++) {
        $table.append("<tr>");
        for (let j = 0; j < column.length; j++) {
            add_row($table, results, column, i, j)
        }
        $table.append("</tr>");
    }
    if (num_results == 0 && column != 'null') {
        $table.empty();
        $table.append("<tr>");
        for (let j = 0; j < column.length; j++) {
            $table.append("<td> no result </td>");
        }
        $table.append("</tr>");
    }

}


function add_row($table, results, column, i, j) {
    let col = column[j].toLowerCase();
    let val = results[i][col];
    if (col === 'status') {
        let err_code = results[i]['status_code'];
        let err_val = val;
        if (err_code != 'null' && Boolean(err_code)) {
            err_val = val + " : " + err_code;
            $table.append("<td class=" + err_code + ">" + err_val +"</td>");
        } else {
            $table.append("<td class=" + val + ">" + err_val + "</td>");
        }
    } else if (col === 'koaid') {
        $table.append("<td class=" + val +
            "><a href=/koarti/header/" + val + ">" +
            val + "</a></td>");
    } else if (col === 'status_code') {
        if (val != '' && val != 'null') {
            $table.append("<td class=" + val + ">" + val + "</td>");
        } else {
            $table.append("<td class=" + val + "> None </td>");
        }
    } else {
        $table.append("<td>" + val + "</td>");
    }
}

$(document).ready(function() {
    load();
});



/**
 * Simple long polling client based on JQuery
 */

/**
 * Request an update to the server and once it has answered, then update
 * the content and request again.
 * The server is supposed to response when a change has been made on data.
 */

function update() {
    console.log('update');
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
    console.log('load');
    $.ajax({
        url: '/koarti/data',
        success: function(data) {
            write_table(data);
            update();
        }
    });
}

/**
 * Perform update of koa_status.reviewed.
 */
function set_reviewed(id, val) {

    var url = new URL(window.location.href);

    let data = {"val":val, "ids":[id]};
    $.ajax({
        url: '/koarti/koa_status/reviewed',
        contentType: 'application/json',
        data: JSON.stringify(data), 
        type: 'PUT',
        success: function(data) {
            console.log('set_reviewed successful');
            window.location.reload(false); 
            //load();
        },
        error: function (textStatus, errorThrown) {
            console.log(textStatus+":"+errorThrown);
        },
        timeout: 5000
    });
}

function set_checked_reviewed(val, key)
{

    //gather ids from checked boxes
    ids = [];
    var inputs = document.getElementsByTagName('input');
    for (var i = 0; i < inputs.length; i++) 
    {
        var input = inputs[i];
        if (input.type != 'checkbox') continue;
        if (input.id.indexOf(key) != 0) continue;
        if (!inputs[i].checked) continue; 
        var id = input.id.substr(key.length);
        ids.push(id);
    }
    if (!ids) return;

    var url = new URL(window.location.href);

    let data = {"val":val, "ids":ids};
    console.log('set_checked_reviewed: '+data);
    $.ajax({
        url: '/koarti/koa_status/reviewed',
        contentType: 'application/json',
        data: JSON.stringify(data), 
        type: 'PUT',
        success: function(data) {
            console.log('set_checked_reviewed successful');
            window.location.reload(false); 
            //load();
            //document.getElementById('checkall').checked = false;
        },
        error: function (textStatus, errorThrown) {
            console.log(textStatus+":"+errorThrown);
        },
        timeout: 5000
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
        add_row_checkbox($table, results[i]);
        for (let j = 0; j < column.length; j++) {
            add_row_col($table, results, column, i, j)
        }
        add_row_action_btns($table, results[i]);
        $table.append("</tr>");
    }

    if (num_results == 0 && column != 'null') {
        $table.empty();
        $table.append("<tr>");
        $table.append("<td></td>");
        for (let j = 0; j < column.length; j++) {
            $table.append("<td> no result </td>");
        }
        $table.append("<td></td>");
        $table.append("</tr>");
    }
    else {
        if (Boolean(results)) {
            add_batch_action_btns($table);
        }
    }
}

function add_batch_action_btns($table)
{
    let htm = "<tr><td align='left' colspan=99>with checked: "
            + "<input type=button value='review' onclick='set_checked_reviewed(1, \"CBrow\");'> "
            + "<input type=button value='unreview' onclick='set_checked_reviewed(0, \"CBrow\");'>"
            + "</td></tr>";
    $table.append(htm);
}

function add_row_checkbox($table, data)
{
    let id = "CBrow"+data['id'];
    $table.append("<td><input type=checkbox id='"+id+"' name='"+id+"'></input></td>");
}

function add_row_col($table, results, column, i, j) {

    //col vals
    let col = column[j].toLowerCase();
    let val = results[i][col];
    let reviewed = results[i]['reviewed'];
    if (col === 'status') {
        let err_code = results[i]['status_code'];
        let err_val = val;
        if (err_code != 'null' && Boolean(err_code)) {
            let cl = err_code;
            if (err_code)
            {
                if (reviewed == 1) cl = '';
                else               cl = `WARN ${val}`;
            }
            err_val = val + "<br>(" + err_code +')';
            $table.append(`<td class='${cl}'>${err_val}</td>`);
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

function add_row_action_btns($table, data)
{
    let dbid = data['id'];
    let status_code = data['status_code'];
    let baseurl = window.location.href.split('?')[0];
    let filepath = `${baseurl}/log/${data['id']}?format=html`;
    let htm = '<td>';
    htm += "<input type=button value='log' onclick='view_log(\""+filepath+"\");'>";
    if (status_code != 'null') 
    {
        var chk = (data['reviewed'] == 1) ? ' checked=checked ' : '';
        var val = (data['reviewed'] == 1) ? 0 : 1;
        htm += `&nbsp;<input type=checkbox ${chk} onchange='set_reviewed(${dbid}, ${val});'>reviewed?</input>`;
    }
    htm += "</td>";
    $table.append(htm);
}

function start_spinner(divId)
{
    if (typeof gSpinner === 'undefined') 
    {
        var div = document.getElementById(divId);
        gSpinner = create_spinner(divId);
        div.style.pointerEvents = 'none';
    }
}

function stop_spinner(divId)
{
    var div = document.getElementById(divId);
    if (gSpinner) 
    {
        gSpinner.stop();
        gSpinner = null;
    }
    div.style.pointerEvents = 'auto';
}

function create_spinner(divId)
{
    var opts = 
    {
        lines: 13,
        length: 50,
        width: 14,
        radius: 40,
        corners: 1,
        rotate: 0,
        color: '#000',
        speed: 1,
        trail: 60,
        shadow: false,
        hwaccel: false,
        className: 'spinner',
        zIndex: 2e9,
        top: '120px',
        left: '50%',
        position: 'relative'
    };
    var div = document.getElementById(divId);
    var spinner = new Spinner(opts).spin(div);
    return spinner;
}



$(document).ready(function() {
    load();
});



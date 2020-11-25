

function set_section(id, val_to_select) {
    // set the selected value of the selection element
    let element = document.getElementById(id);
    element.value = val_to_select;
}

function disable(select_id) {
    // disable element,  do not add values to the request variables
    if (Array.isArray(select_id)) {
        let arrayLength = select_id.length;
        for (let i = 0; i < arrayLength; i++) {
            document.getElementById(select_id[i]).disabled = true;
        }
    } else {
        document.getElementById(select_id).disabled = true;
    }

}

function enable(select_id) {
    // enable element,  add values to the request variables
    if (Array.isArray(select_id)) {
        let arrayLength = select_id.length;
        for (let i = 0; i < arrayLength; i++) {
            document.getElementById(select_id[i]).disabled = false;
        }
    } else {
        document.getElementById(select_id).disabled = false;
    }

}

function check(chk_name) {
    // set the checkbox to checked
    document.getElementById(chk_name).checked = true;
}

function block_div(div_name) {
    // hide a block of html within a div
    if (Array.isArray(div_name)) {
        let arrayLength = div_name.length;
        for (let i = 0; i < arrayLength; i++) {
            document.getElementById(div_name[i]).style.display = 'none';
        }
    } else {
        document.getElementById(div_name).style.display = 'none';
    }
}

function enable_div(div_name) {
    // show a block of html within a div
    if (Array.isArray(div_name)) {
        let arrayLength = div_name.length;
        for (let i = 0; i < arrayLength; i++) {
            document.getElementById(div_name[i]).style.display = 'block';
            document.getElementById(div_name[i]).style.display = 'inline';
        }
    } else {
        document.getElementById(div_name).style.display = 'block';
        document.getElementById(div_name).style.display = 'inline';    }
}
function check_enable(chk, change_str) {
    // enable / disable values associated to the checkbox element
    let checkBox = document.getElementById(chk);
    let to_change = change_str.split(', ')

    if (checkBox.checked === true){
        enable(to_change)
    } else {
        disable(to_change)
    }
}

function hide_report() {
    let table_id = document.getElementById('show_table');
    if (table_id.style.display === "none") {
        enable_div("show_table")
    } else {
        block_div("show_table")
    }
}

function update_input_value(search_val, utd, val, opt_lists) {

    if (search_val === 'STATUS' || search_val === 'Image_Type') {
        enable("enter_value");
        disable("val");
        block_div("enter_val")
        enable_div("enter_opts")

        set_section('enter_value', val);
        update_pulldown(search_val, opt_lists, val);
    }
    else {
        enable("val");
        disable("enter_value");
        block_div("enter_opts")
        enable_div("enter_val")

        let default_val = '';
        if (search_val === 'PI') {
            default_val = 'Lastname';
        } else if (search_val === 'KOAID') {
            let short_utd = utd.replace('-','');
            default_val = 'HI.' + short_utd + '.08424';
        } else if (search_val === 'Program_ID') {
            default_val = 'U088';
        } else if (search_val === 'SEMID') {
            let month = utd.substring(4, 6);
            let sem = 'B';
            if (month > 1 && month < 8) {
                sem = 'A';
            }
            let yr = utd.substring(0, 4);
            default_val = yr + sem;
        }
        set_section('val', default_val)
    }
    set_section('search', search_val)
}

function change_options(val, inst_lists, on_load) {
    let tel = document.getElementById("tel").value;

    if (val === 'daily') {
        let to_block = ['monthly_opts', 'plots'];
        let to_enable = ['tel_opts', 'search_opts', 'view_opts', 'inst_opts',
                         'date_opts'];
        block_div(to_block);
        enable_div(to_enable);

        to_enable = ["chk", "search", "val", "chk", "view", "tel"];
        to_block = ["yr", "month", "plot"];
        disable(to_block);
        enable(to_enable)
        if (on_load == 0) {
            check("chk")
        }
        check_enable("chk", 'utd, utd2')
    } else if (val === 'monthly') {
        let to_block = ['search_opts', 'view_opts', 'date_opts', 'plots'];
        let to_enable = ['monthly_opts', 'tel_opts', 'inst_opts'];
        block_div(to_block);
        enable_div(to_enable);

        to_block = ["search", "val", "view", "utd", "utd2", "plot", "chk"];
        to_enable = ["yr", "month", "tel"];
        disable(to_block);
        enable(to_enable);

    } else if (val === 'health') {
        let to_block = ['search_opts', 'view_opts', 'monthly_opts',
                        'inst_opts', 'date_opts', 'plots', 'tel_opts', 'inst_opts'];
        block_div(to_block);

        to_block = ["search", "val", "view", "utd", "utd2", "yr",
                    "month", "plot", "tel", "inst", "chk"];
        disable(to_block);

    } else {
        let to_block = ['search_opts', 'view_opts', 'monthly_opts'];
        let to_enable = ['date_opts', 'plots'];
        block_div(to_block);
        enable_div(to_enable);

        to_block = ["search", "val", "view", "yr", "month"];
        to_enable = ["chk", "utd", "utd2", "tel", "inst", "plot"];
        disable(to_block);
        enable(to_enable);
        if (on_load == 0) {
            check("chk")
        }
    }

    // needed to update the list of instruments since no telescope selector
    update_inst(tel, "0", inst_lists);

}

function update_options(qselect, select_opts) {
    let arrayLength = select_opts.length;
    for (let i = 0; i < arrayLength; i++) {
        let opt1 = document.createElement('option');
        opt1.value = select_opts[i];
        opt1.innerHTML = select_opts[i];
        qselect.appendChild(opt1);
    }
}

function update_inst(tel, inst_val, inst_lists) {
    if (inst_val == "0") {
        inst_val = document.getElementById("inst").value;
    }
    let sel_inst = document.querySelector('#inst');
    sel_inst.innerHTML = '';

    let inst_list;
    if (tel == 1) {
        inst_list = inst_lists[0];
    } else if (tel == 2) {
        inst_list = inst_lists[1];
    } else {
        inst_lists[1].shift();
        inst_list = inst_lists[0].concat(inst_lists[1]);
    }

    update_options(sel_inst, inst_list);
    set_section("inst", inst_val);

}

function update_pulldown(search_val, opt_lists, val) {
    let sel_enter = document.querySelector('#enter_value');
    sel_enter.innerHTML = '';
    let enter_opts;
    if (search_val === "Image_Type") {
        enter_opts = opt_lists['img_type'];
    } else {
        enter_opts = opt_lists['status'] ;
    }

    update_options(sel_enter, enter_opts);
    set_section("enter_value", val);

}


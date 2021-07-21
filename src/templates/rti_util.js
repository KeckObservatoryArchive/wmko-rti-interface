
function checkAll(elm, key) 
{
    var inputs = document.getElementsByTagName('input');
    for (var i = 0; i < inputs.length; i++) 
    {
        var input = inputs[i];
        if (input.type != 'checkbox') {continue;}
        if (input.id.indexOf(key) != 0) {continue;}
        inputs[i].checked = elm.checked;
    }
}

function view_log(filepath)
{
    console.log(filepath);
    window.open(filepath, 'view log', 'width=800, height=650, status=yes, resizable=yes, scrollbar=yes');
}
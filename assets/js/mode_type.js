var table_mt = document.getElementById("table_mt");
var accordion = document.getElementById("accordion");
var ModeType = [];


function viewModeType() {
    let table_content = "";
    table_content+="<tbody>";
    for(let i = 0; i<ModeType.length; i++) {
        table_content+="<tr>";
        table_content+="<td>"+ ModeType[i].ID +"</td>";
        table_content+="<td>"+ ModeType[i].Info +"</td>"; //Info
        table_content+="<td>"+ ModeType[i].Type_Name+" ["+ ModeType[i].Type_ID +"] "+"</td>"; //type
        table_content+="<td>"+ ModeType[i].Mod_Name+" ["+ ModeType[i].Mod_ID +"] "+"</td>"; //mod
        table_content+="<td>";
        table_content+="<input type='button'  class='btn btn-xs btn-info edit_info' id="+i+ " value='Редактировать'>";
        table_content+="<br>";
        table_content+="<button type='button' class='btn btn-xs btn-danger delete_mt_new' id="+i+ " >Удалить</button>";
        table_content+="</td>";
        table_content+="</tr>";
    }
    table_content+="</tbody>";
    table_mt.innerHTML = table_content;


}





$(document).on('click',".edit_info",function() {
    $("#ModalEditInfo").modal('show'); //
    $("#edit_info_text").val(ModeType[+this.id].Info);

    $(".classtype").text("Тип:  "+ModeType[+this.id].Type_Name);
    $(".classmode").text("Мод:  "+ModeType[+this.id].Mod_Name);
    $(".Tab_Name").text("Tab_Name:  "+ModeType[+this.id].Tab_Name);

    $(".save_mt").prop("id",+this.id);
    //classtype
});

$(document).on('click',".save_mt",function() {

    let newInfo = $("#edit_info_text").val();
    let idTypeMode= this.id;
    if(newInfo != "")
    {
        ModeType[+idTypeMode].Info = newInfo;
    }
    else
    {
        alert("Введите описание");
        return;
    }

    let newParentType = getSelectedRadioId("type");
    let newParentMode = getSelectedRadioId("mode");


    if(newParentType != undefined )
    {
        ModeType[+idTypeMode].Type_ID = newParentType;
    }
    if(newParentMode != undefined )
    {
        ModeType[+idTypeMode].Mod_ID = newParentMode;
    }



    let TableArray = [];
    TableArray.push( JSON.stringify(new Struct_Table("Update", "metrics_link_type_and_mod", "Id", 99999, 0,
        [+ModeType[+idTypeMode].ID,
            ModeType[+idTypeMode].Type_ID,
            +ModeType[+idTypeMode].Mod_ID,
            ModeType[+idTypeMode].Info])));

    Link_action_obj.Action('ready',TableArray);

    ///ajax save row
    $("#ModalEditInfo").modal('hide'); //ModalEditInfo

    console.log('Сохранение mt id:',this.id);
    uncheckRadio("type");
    uncheckRadio("mode");
});
$(document).on('click',".add_mt",function() {
    $("#ModalAddInfo").modal('show'); //
});

$(document).on('click',".add_mt_info",function() {
    let newInfo = $("#add_info_text").val();

    let idType = getSelectedRadioId("type");
    let idMode = getSelectedRadioId("mode");
    if(!idType || !idMode) {
        alert("Не выбран тип или мод");
        return;
    }
    //создаем запись
    let TableArray = [];
    TableArray.push( JSON.stringify(new Struct_Table("Insert", "metrics_link_type_and_mod", "", 99999, 0, [+idType,+idMode,newInfo])));

    Link_action_obj.Action('ready',TableArray);

    $("#ModalAddInfo").modal('hide'); //
});
$(document).on('click',".delete_mt_new",function() {
    let TableArray = [];
    TableArray.push( JSON.stringify(new Struct_Table("Delete", "metrics_link_type_and_mod", "Id", 99999, 0, [+ModeType[+this.id].ID])));

    Link_action_obj.Action('ready',TableArray);
});
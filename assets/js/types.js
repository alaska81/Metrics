var table_types = document.getElementById("chain");
var select_types = document.createElement("SELECT");
var ul_chains = document.getElementById("chain_ul"); //
var chain_ul_test = document.getElementById("chain_ul_test");
var chain_ul_test2 = document.getElementById("chain_ul_test2");
var Types = [];
var Chains = []; //цепи
var GroupsChains = {}; //Группы цепей
var insteringId = 0;
var isroot = false;
var str_html = "";
function addType(id) {
    $("#myModalLabel").text("Добавить дочерний тип к:      " + getNameType(id));
    $(".save").prop("id",id);
    $(".save").prop("value","type");
}

function recViewGroup2(parent,tp,isLast) { //

    if(parent.children)
    {
        if(isroot)
        {
            str_html += "<li class='Node IsRoot ExpandOpen'> <div class='Expand'></div> </div> <div class='Content' style='font-weight: bold'>" +
                "<input type='radio' name="+tp+" value=" + parent.ID + ">"  +
                parent.Name;
            isroot = false;
        }
        else
        {
            if(!isLast)
            {
                str_html += "<li class='Node ExpandOpen'> <div class='Expand'></div> </div> <div class='Content' style='font-weight: bold'>" +
                    "<input type='radio' name="+tp+" value=" + parent.ID + ">"  +
                    parent.Name;
            }
            else
            {
                str_html += "<li class='Node ExpandOpen IsLastNode'> <div class='Expand'></div> </div> <div class='Content' style='font-weight: bold'>" +
                    "<input type='radio' name="+tp+" value=" + parent.ID + ">"  +
                    parent.Name;
            }
        }




        if(tp == "type")
        {
            str_html +=" <button onclick='addType(this.id)' type='button' class='add' id=" + parent.ID + " data-toggle='modal' data-target='#myModal' title='Добавить'><img  class='action_image' src='assets/image/add.png' alt='add' ></button>"+
                " <button type='button' value="+tp+" class='delete' id=" + parent.ID + " title='Удалить'><img  class='action_image' src='assets/image/001-x-button.png' alt='delete' ></button>" +
                " <button type='button' value="+tp+" class='move' id=" + parent.ID + " title='Сменить родителя'><img  class='action_image' src='assets/image/horizontal-arrows.png' alt='move' ></button>";

        }
        else if(tp == "mode")
        {
            str_html +=" <button onclick='addModifier(this.id)' type='button' class='add' id=" + parent.ID + " data-toggle='modal' data-target='#myModal' title='Добавить'><img  class='action_image' src='assets/image/add.png' alt='add' ></button>"+
                " <button type='button' value="+tp+" class='delete' id=" + parent.ID + " title='Удалить'><img  class='action_image' src='assets/image/001-x-button.png' alt='delete' ></button>" +
                " <button type='button' value="+tp+" class='move' id=" + parent.ID + " title='Сменить родителя'><img  class='action_image' src='assets/image/horizontal-arrows.png' alt='move' ></button>";

        }



        //IsLastNode

        str_html += " </div> <ul class='Container'>";

        for(let i = 0; i<parent.children.length; i++)
        {
            if(i != parent.children.length-1) //LAST!
                recViewGroup2(parent.children[i],tp,false); //запросили предков
            else recViewGroup2(parent.children[i],tp,true); //запросили предков
        }

        str_html += "</ul>";
        str_html += "</li>";
    }
    else
    {
        if(isLast)
        str_html += "<li class='Node ExpandLeaf IsLast'> <div class='Expand'></div> <div class='Content' style='font-style: oblique;'>" +
            "<input type='radio' name="+tp+" value=" + parent.ID + ">"  +
            parent.Name;
        else
        {
            str_html += "<li class='Node ExpandLeaf'> <div class='Expand'></div> <div class='Content' style='font-style: oblique;'>" +
                "<input type='radio' name="+tp+" value=" + parent.ID + ">"  +
                parent.Name;
        }

        if(tp == "type")
        {
            str_html +=" <button onclick='addType(this.id)' type='button' class='add' id=" + parent.ID + " data-toggle='modal' data-target='#myModal' title='Добавить'><img  class='action_image' src='assets/image/add.png' alt='add' ></button>"+
                " <button type='button' value="+tp+" class='delete' id=" + parent.ID + " title='Удалить'><img  class='action_image' src='assets/image/001-x-button.png' alt='delete' ></button>" +
                " <button type='button' value="+tp+" class='move' id=" + parent.ID + " title='Сменить родителя'><img  class='action_image' src='assets/image/horizontal-arrows.png' alt='move' ></button>";

        }
        else if(tp == "mode")
        {
            str_html +=" <button onclick='addModifier(this.id)' type='button' class='add' id=" + parent.ID + " data-toggle='modal' data-target='#myModal' title='Добавить'><img  class='action_image' src='assets/image/add.png' alt='add' ></button>"+
                " <button type='button' value="+tp+" class='delete' id=" + parent.ID + " title='Удалить'><img  class='action_image' src='assets/image/001-x-button.png' alt='delete' ></button>" +
                " <button type='button' value="+tp+" class='move' id=" + parent.ID + " title='Сменить родителя'><img  class='action_image' src='assets/image/horizontal-arrows.png' alt='move' ></button>";

        }


        str_html += " </div>  </li>";
    }

    //запись в HTML
    if(tp == "type")
    {
        chain_ul_test.innerHTML = str_html;
        chain_ul_test2.innerHTML = str_html;
    }
    else if(tp == "mode")
    {
        ul_chains_Modes.innerHTML = str_html;
        ul_chains_Modes2.innerHTML = str_html;
    }
}

function viewChains() {
    //str_html="";
    //recViewGroup(GroupsChains[0]);
    str_html="";
    isroot = true;
    recViewGroup2(GroupsChains[0],"type");

}

function getParentType(id) {
    if(id == 0) return {ID:0,Name:"Типы", children:[]};
    for(let i in Types) {
        if (id == Types[i].ID) {
            return Types[i];
        }
    }
    console.error("не найден родитель id=", id);
    return undefined;
}
function getNameType(id) {
    if(id == 0) return 0;
    for(let i in Types) {

        if (id == Types[i].ID) {
            return Types[i].Name;
        }
    }
    console.error("не найден родитель id=", id);
    return undefined;
}



function getChildren(id) {
    let children = [];
    for(let i = 0; i<Types.length; i++)
    {
        if(id == Types[i].Parent_ID)
        {
            children.push(Types[i]);
        }
    }
    return children;
}
function recGroup(parent) { //работает корректно
    let children;
    if(parent.Parent_ID == undefined)
        children = getChildren(0);
    else
        children = getChildren(parent.ID);


    if(children.length>0)
    {
        parent.children = children;
        for(let i = 0; i<children.length; i++)
        {
            let newparent = parent.children[i];
            recGroup( newparent ); //запросили предков
        }
    }
    else
    {

    }
}

function parseChains() {
    Chains = [];

    for(let i = 0; i<Types.length; i++)
    {
        let chain = [];
        chain.push(Types[i]);
        let nextParent = true;
        let count = 10;
        while(nextParent) //поиск родителей
        {
            if(count > 0)
            {
                count--;
            }
            else {
                console.error('Ошибка, превышен интервал шагов поиска родителей',Types[i]);
                nextParent = false;
                break;

            }

            let temp = getParentType(chain[chain.length-1].Parent_ID);//находим родителя последнего потомкка в цепи
            if(temp == undefined) {

                nextParent = false;
                break;
            }
            chain.push(temp);
            if(temp == 0) nextParent = false;
        }
        Chains.push(chain);
    }

}
function groupChains() {
    GroupsChains = {};
    GroupsChains[0] = getParentType(0);
    recGroup(GroupsChains[0]); //структурирование типов
}



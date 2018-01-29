var ul_chains_Modes = document.getElementById("chain_ul_modes");
var ul_chains_Modes2 = document.getElementById("chain_ul_modes2");
var Modes = [];
var Chains_Modes = []; //цепи
var GroupsChains_Modes = {}; //Группы цепей

function addModifier(id) {
    $("#myModalLabel").text("Добавить дочерний модификатор к:   " + getName_Modes(id));
    $(".save").prop("id",id);
    $(".save").prop("value","mode");
}

function viewChains_Modes() {
    // str_html_Modes="";
    // recViewGroup_Modes(GroupsChains_Modes[0]);

    str_html="";
    isroot = true;
    recViewGroup2(GroupsChains_Modes[0],"mode");
}

function getParent_Modes(id) {
    if(id == 0) return {ID:0,Name:"Модификаторы", children:[]};
    for(let i in Modes) {
        if (id == Modes[i].ID) {
            return Modes[i];
        }
    }
    console.error("не найден родитель модификатора id=", id);
    return undefined;
}
function getName_Modes(id) {
    if(id == 0) return 0;
    for(let i in Modes) {

        if (id == Modes[i].ID) {
            return Modes[i].Name;
        }
    }
    console.error("не найден родитель id=", id);
    return undefined;
}



function getChildren_Modes(id) {
    let children = [];
    for(let i = 0; i<Modes.length; i++)
    {
        if(id == Modes[i].Parent_ID)
        {
            children.push(Modes[i]);
        }
    }
    return children;
}
function recGroup_Modes(parent) { //работает корректно
    let children;
    if(parent.Parent_ID == undefined)
        children = getChildren_Modes(0);
    else
        children = getChildren_Modes(parent.ID);


    if(children.length>0)
    {
        parent.children = children;
        for(let i = 0; i<children.length; i++)
        {
            let newparent = parent.children[i];
            recGroup_Modes( newparent ); //запросили предков
        }
    }
}

function parseChains_Modes() {
    Chains_Modes = [];

    for(let i = 0; i<Modes.length; i++)
    {
        let chain = [];
        chain.push(Modes[i]);
        let nextParent = true;
        let count = 10;
        while(nextParent) //поиск родителей
        {
            if(count > 0)
            {
                count--;
            }
            else {
                console.error('Ошибка, превышен интервал шагов поиска родителей',Modes[i]);
                nextParent = false;
                break;

            }

            let temp = getParent_Modes(chain[chain.length-1].Parent_ID);//находим родителя последнего потомкка в цепи
            if(temp == undefined) {

                nextParent = false;
                break;
            }
            chain.push(temp);
            if(temp == 0) nextParent = false;
        }
        Chains_Modes.push(chain);
    }

}
function groupChains_Modes() {
    GroupsChains_Modes = {};
    GroupsChains_Modes[0] = getParent_Modes(0);
    recGroup_Modes(GroupsChains_Modes[0]); //структурирование модификаторов
}


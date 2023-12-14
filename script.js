
let uID = null

const getUID = async() => {
    uID  = document.getElementById('uIDInput').value;
    document.getElementById('uIDcurrent').innerHTML = uID
    document.getElementById('authForm').style.display ='none'
    return uID
}


const register = async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  login  = document.getElementById('login').value;
    const  password  = document.getElementById('password').value;
    const prms = new URLSearchParams({
        login: login,
        password: password
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/register?" + prms, {method : "POST"});
    document.getElementById('response').innerHTML = await response.text()
    document.getElementById('registrationForm').style.display ='none'
}




function openAddWTab(){
    const state = document.getElementById('addPlanTab').style.display
    if (state == 'flex'){
        document.getElementById('addPlanTab').style.display ='none'
    } else {
        document.getElementById('addPlanTab').style.display ='flex'
    }
}

const randomQuote = async() =>{
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/quote", {method : "GET"});
    const json = await response.text();
    const obj = JSON.parse(json);
    document.getElementById('response').innerHTML = "<div id = 'quoteResp'>In anime " + obj.anime+ " character " +obj.character+ " said: <p>" + obj.quote+ "</p> </div>"
    console.log(response)
}


const searching = async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  s_q  = document.getElementById('search').value;
    const prms = new URLSearchParams({
        s_query: s_q
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/search?" + prms, {method : "GET"});
    document.getElementById('response').innerHTML = await response.text()
    console.log(response)
}


const addToWishlist = async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  uID_param  = uID;
    const  animeID  = document.getElementById('animeIDaddWL').value;
    const prms = new URLSearchParams({
        uID: uID_param,
        animeID: animeID
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/addToWishlist?" + prms, {method : "POST"});
    document.getElementById('response').innerHTML = await response.text()

}

const getWishlist = async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  uID_param  = uID;
    const prms = new URLSearchParams({
        uID: uID_param
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/getWishlist?" + prms, {method : "GET"});
    document.getElementById('response').innerHTML = await response.text()
    console.log(response)
}

const delFromWish = async(id) =>{
    const wishID = uID + "_" + id
    const prms = new URLSearchParams({
        wishID: wishID
    })
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/deleteWish?" + prms, {method : "DELETE"});
    await getWishlist()
    console.log(response)
}

function openMoveWatched(id){
    const form_id = id+ "_rateForm"
    const state = document.getElementById(form_id).style.display
    if (state == 'inline-block'){
        document.getElementById(form_id).style.display ='none'
    } else {
        document.getElementById(form_id).style.display ='inline-block'
    }
}

const moveWatched = async(id) =>{
    const inp_id = id+ "_rate"
    const  animeRate  = document.getElementById(inp_id).value;
    const prms = new URLSearchParams({
        animeRate: animeRate,
        uID: uID,
        animeID: id

    })
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/postWatched?" + prms, {method : "POST"});
    await delFromWish(id)
    console.log(response)
}

function openAddWachedTab(){
    const state = document.getElementById('addWachedTab').style.display
    if (state == 'flex'){
        document.getElementById('addWachedTab').style.display ='none'
    } else {
        document.getElementById('addWachedTab').style.display ='flex'
    }
}

const getWachedlist = async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  uID_param  = uID;
    const prms = new URLSearchParams({
        uID: uID_param
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/getWatchedlist?" + prms, {method : "GET"});
    document.getElementById('response').innerHTML = await response.text()
    console.log(response)
}

const addToWachedlist= async() => {
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  uID_param  = uID;
    const  animeID  = document.getElementById('animeIDaddWachedL').value;
    const  animeRate  = document.getElementById('animeRateaddWachedL').value;
    const prms = new URLSearchParams({
        uID: uID_param,
        animeID: animeID,
        animeRate: animeRate
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/postWatched?" + prms, {method : "POST"});
    document.getElementById('response').innerHTML = await response.text()
}


const delFromWatch = async(id) =>{
    const watchID = uID + "_" + id
    const prms = new URLSearchParams({
        watchID: watchID
    })
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/deleteWatched?" + prms, {method : "DELETE"});
    await getWachedlist()
    console.log(response)
}

const recomList = async() =>{
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  uID_param  = uID;
    const prms = new URLSearchParams({
        uID: uID_param
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/recom?" + prms, {method : "GET"});
    document.getElementById('response').innerHTML = await response.text()
    console.log(response)
}

function openForgetUID(){
    const state = document.getElementById('remindForm').style.display
    if (state == 'inline-block'){
        document.getElementById('remindForm').style.display ='none'
    } else {
        document.getElementById('remindForm').style.display ='inline-block'
    }
}

const remind = async() =>{
    document.getElementById('response').innerHTML = "The content is loading, please wait"
    const  login  = document.getElementById('loginRemind').value;
    const  password  = document.getElementById('passwordRemind').value;
    const prms = new URLSearchParams({
        login: login,
        password: password
    })
    const response = await fetch("https://d5dmaki73l0hentlufcr.apigw.yandexcloud.net/remind?" + prms, {method : "GET"});
    document.getElementById('response').innerHTML = await response.text()
    document.getElementById('remindForm').style.display ='none'
}

function openPage(pageName) {
    // Hide all elements with class="tabcontent" by default */
    var i, tabcontent;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
      tabcontent[i].style.display = "none";
    }
    document.getElementById(pageName).style.display = "flex";
    document.getElementById('response').innerHTML = ''
}
  
  // Get the element with id="defaultOpen" and click on it
document.getElementById("firstTab").click();


// on https://www.cs.csubak.edu/~msarr/visualizations/BPlusTree.html

let add = async (n, straight) => {
    let array = Array.from({length: n}, (_, i) => i + 1)
    let reverseArray = array.slice().reverse();
    let m = array

    if (!straight) {
        m = reverseArray
    }
    for (let i = 0; i < m.length; i++) {
        document.getElementsByTagName('input')[0].value = '' + m[i];
        await new Promise(r => setTimeout(r, 500));
        document.getElementsByName('Insert')[0].click();
    }
}


let del = async (array) => {
    for (let i = 0; i < array.length; i++) {
        document.getElementsByTagName('input')[2].value = '' + array[i];
        await new Promise(r => setTimeout(r, 500));
        document.getElementsByName('Delete')[0].click();
    }
}

delarr = [1, 2, 3, 4, 5, 6, 10, 11, 13, 12, 14, 15, 16, 17, 18, 40, 31, 35, 33, 30]

function redirect_to(step_num) {
    var dict = {
        1: '/movie/',
        2: '/movie/',
        3: '/rating/',
        4: '/genre/',
        5: '/movie/',
        6: '/movie/',
    }
    if ([1, 3, 4].includes(step_num)) {
        var input_step = document.getElementById('step_' + step_num).value;
        location.href = dict[step_num] + input_step;
    } else if ([5].includes(step_num)) {
        var input_step_5_1 = document.getElementById('step_' + step_num + '_1').value;
        var input_step_5_2 = document.getElementById('step_' + step_num + '_2').value;
        location.href = dict[step_num] + input_step_5_1 + '/' + input_step_5_2;
    } else if ([2].includes(step_num)) {
        var input_step_2_1 = document.getElementById('step_' + step_num + '_1').value;
        var input_step_2_2 = document.getElementById('step_' + step_num + '_2').value;
        location.href = dict[step_num] + input_step_2_1 + '/to/' + input_step_2_2;
    } else {
        var input_step_6_1 = document.getElementById('step_' + step_num + '_1').value;
        var input_step_6_2 = document.getElementById('step_' + step_num + '_2').value;
        var input_step_6_3 = document.getElementById('step_' + step_num + '_3').value;
        location.href = dict[step_num] + input_step_6_1 + '/' + input_step_6_2 + '/' + input_step_6_3;
    }
}

def get_modal_name(fpaths): 
    return max(set(fpaths), key=fpaths.count)

# a = [1,1,2,2]

# print get_modal_name(a)


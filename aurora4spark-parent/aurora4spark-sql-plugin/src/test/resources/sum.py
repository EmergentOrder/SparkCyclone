numbers = [1,2,3]
from nlcpy import veo
import os
import sys

nodeid = os.environ.get("VE_NODE_NUMBER", 0)
bld = veo.VeBuild()
bld.set_build_dir("_ve_build")
bld.set_c_src("_sum", r"""

double sum(double *a, int n)
{
    int i;
    double sum = 0;
    for (i = 0; i < n; i++) {
        sum += a[i];
    }

    return sum;
}""")

ve_so_name = bld.build_so()

proc = veo.VeoAlloc().proc
ctxt = proc.open_context()
lib = proc.load_library((os.getcwd() + "/" + ve_so_name).encode("UTF-8"))
lib.sum.args_type(b"double*", "int")
lib.sum.ret_type("double")
np = veo.np

np_numbers = np.array(numbers)
a_ve = proc.alloc_mem(len(numbers) * 8)
proc.write_mem(a_ve, np_numbers, len(numbers) * 8)
req = lib.sum(ctxt, a_ve, len(numbers))
sum = req.wait_result()

proc.free_mem(a_ve)
del proc

sum
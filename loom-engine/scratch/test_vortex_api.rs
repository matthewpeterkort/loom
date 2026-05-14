use vortex::array::ArrayRef;
use vortex::array::arrays::StructArray;
use vortex::array::IntoArray;
use vortex::dtype::FieldNames;
use vortex::array::validity::Validity;
use vortex::array::variants::StructArrayTrait;

fn main() {
    let names = FieldNames::from(vec!["a".to_string()]);
    let col = vortex::array::arrays::VarBinArray::from(vec!["hello".to_string()]).into_array();
    let struct_arr = StructArray::try_new(names, vec![col], 1, Validity::NonNullable).unwrap().into_array();
    
    // Test casting
    let s = struct_arr.as_struct().unwrap();
    println!("Field count: {}", s.nfields());
}

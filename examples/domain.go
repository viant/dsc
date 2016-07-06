package examples

//Interest represents an generic interest.
type Interest struct {
	ID        int
	Name      string
	Category  string
	Status    *bool  `valueMap:"yes:true,no:false"`
	GroupName string `transient:"true"`
}

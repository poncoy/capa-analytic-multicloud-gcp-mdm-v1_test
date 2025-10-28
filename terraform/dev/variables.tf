variable "bigtable_instances" {
  type = map(object({    
    num_nodes    = number
    storage_type = string    
  }))
}


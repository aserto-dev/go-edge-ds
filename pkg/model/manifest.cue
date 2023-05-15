// manifest.cue
import "encoding/yaml"

#ObjectTypes: {
    [string]: *null | #ObjectType
    
    #ObjectType: {
        [string]?: *null | #Relation
    }

    #Relation: {
        union?: *null | [...string]
        permissions?: *null | [...string]
    }
}

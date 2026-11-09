//! Step Functions behavior replayed against what AWS itself returned for the same
//! definitions: what creation refuses and why, and for what runs, its status,
//! output, error, cause and history event types.

mod helpers;

use aws_sdk_sfn::error::ProvideErrorMetadata;
use aws_sdk_sfn::types::ExecutionStatus;
use helpers::TestServer;
use serde_json::Value;

const ROLE: &str = "arn:aws:iam::123456789012:role/fc-probe";

/// `{name, definition, recorded}`, as recorded from AWS.
const RECORDED: &str = r###"[
 {
  "name": "delim_spaces",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": " {% 1 %} "
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\" {% 1 %} \"",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "delim_open_only",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 1"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: JSONata string must start with {% and end with %} at /States/A/Output'"
  }
 },
 {
  "name": "delim_text_around",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": "a {% 1 %} b"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"a {% 1 %} b\"",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "delim_inner",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "x": "{% 1 %} tail"
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: JSONata string must start with {% and end with %} at /States/A/Output/x'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_parameters",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Parameters": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'Parameters' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_inputpath",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "InputPath": "$"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'InputPath' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_in_jsonata_resultpath",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "ResultPath": "$.x"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'ResultPath' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jp_dotdollar",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "a.$": "$.b"
     }
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "{\"a.$\":\"$.b\"}",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "mix_jsonata_in_jp_output",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Output": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONPath', but field 'Output' is only supported for the 'JSONata' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "mix_jsonata_in_jp_args",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONPath', but field 'Arguments' is only supported for the 'JSONata' QueryLanguage at /States/A, SCHEMA_VALIDATION_FAILED: Parameters field is required for resource ARN: arn:aws:states:::aws-sdk:sfn:listStateMachines at /States/A/Resource'"
  }
 },
 {
  "name": "state_level_ql_jp_in_jsonata",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONPath",
     "Parameters": {
      "a": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: 'QueryLanguage' can not be 'JSONPath' if set to 'JSONata' for whole state machine at /States/A, SCHEMA_VALIDATION_FAILED: The QueryLanguage is set to 'JSONata', but field 'Parameters' is only supported for the 'JSONPath' QueryLanguage at /States/A'"
  }
 },
 {
  "name": "state_level_ql_jsonata_in_jp",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONata",
     "Output": "{% 1 %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "1",
   "error": null,
   "cause": null
  }
 },
 {
  "name": "jsonpath_state_in_jsonata_top_downgrade",
  "definition": {
   "QueryLanguage": "JSONPath",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "QueryLanguage": "JSONata",
     "Output": "{% 1 %}",
     "Next": null
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: There should only be one of the following fields: [Next, End] at /States/A'"
  }
 },
 {
  "name": "retry_task_args",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_task_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_task_assign",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Assign": {
      "x": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Assign/x' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_parallel_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateStarted",
    "PassStateEntered",
    "PassStateExited",
    "ParallelStateSucceeded",
    "EvaluationFailed",
    "ParallelStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_parallel_args",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Parallel",
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true
        }
       }
      }
     ],
     "Arguments": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "ParallelStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ParallelStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_map_items",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% $states.input.missing %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Items' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "MapStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_map_output",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% [1] %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Output": "{% $states.input.missing %}",
     "Retry": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Output' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateStarted",
    "MapIterationStarted",
    "PassStateEntered",
    "PassStateExited",
    "MapIterationSucceeded",
    "MapStateSucceeded",
    "EvaluationFailed",
    "MapStateFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "retry_named_query_err",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Retry": [
      {
       "ErrorEquals": [
        "States.QueryEvaluationError"
       ],
       "MaxAttempts": 2,
       "IntervalSeconds": 1
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "catch_taskfailed_query",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.TaskFailed"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Arguments/maxResults' returned nothing (undefined)."
  }
 },
 {
  "name": "catch_all_query_pass",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "Output": "{% $states.input.missing %}",
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Field 'Catch' is not supported at /States/A'"
  }
 },
 {
  "name": "catch_all_query_task_out",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {},
     "Output": "{% $states.input.missing %}",
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"caught\"",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "TaskScheduled",
    "TaskStarted",
    "TaskSucceeded",
    "EvaluationFailed",
    "TaskStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_named_query_map",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Map",
     "Items": "{% $states.input.missing %}",
     "ItemProcessor": {
      "StartAt": "X",
      "States": {
       "X": {
        "Type": "Pass",
        "End": true
       }
      }
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.QueryEvaluationError"
       ],
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "\"caught\"",
   "error": null,
   "cause": null,
   "types": [
    "ExecutionStarted",
    "MapStateEntered",
    "EvaluationFailed",
    "MapStateFailed",
    "MapStateExited",
    "PassStateEntered",
    "PassStateExited",
    "ExecutionSucceeded"
   ]
  }
 },
 {
  "name": "catch_assign_bad",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Task",
     "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
     "Arguments": {
      "maxResults": "{% $states.input.missing %}"
     },
     "Catch": [
      {
       "ErrorEquals": [
        "States.ALL"
       ],
       "Assign": {
        "y": "{% $states.input.missing %}"
       },
       "Next": "H"
      }
     ],
     "End": true
    },
    "H": {
     "Type": "Pass",
     "End": true,
     "Output": "{% 'caught' %}"
    }
   }
  },
  "recorded": {
   "status": "FAILED",
   "output": null,
   "error": "States.QueryEvaluationError",
   "cause": "An error occurred while executing the state 'A' (entered at the event id #2). The JSONata expression '$states.input.missing' specified for the field 'Catch[0]/Assign/y' returned nothing (undefined).",
   "types": [
    "ExecutionStarted",
    "TaskStateEntered",
    "EvaluationFailed",
    "EvaluationFailed",
    "ExecutionFailed"
   ]
  }
 },
 {
  "name": "wait_no_duration",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: This object should have only one of the following fields: [SecondsPath, Seconds, TimestampPath, Timestamp] at /States/A'"
  }
 },
 {
  "name": "wait_two_durations",
  "definition": {
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Wait",
     "Seconds": 1,
     "Timestamp": "2026-01-01T00:00:00Z",
     "End": true
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: There should only be one of the following fields: [SecondsPath, Seconds, TimestampPath, Timestamp] at /States/A'"
  }
 },
 {
  "name": "var_reserved",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": {
      "states": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'RESERVED_VARIABLE_NAME: Cannot assign to reserved variable '$states'. at /States/A'"
  }
 },
 {
  "name": "var_bad_name",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": {
      "1x": 1
     }
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'INVALID_VARIABLE_NAME: Invalid variable name '1x': the variable name contains invalid characters. at /States/A/Assign/1x'"
  }
 },
 {
  "name": "assign_nonobject",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "A",
   "States": {
    "A": {
     "Type": "Pass",
     "End": true,
     "Assign": 1
    }
   }
  },
  "recorded": {
   "create_error": "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED: Expected value of type [OBJECT] at /States/A/Assign'"
  }
 },
 {
  "name": "sync_no_template_parallel_var",
  "definition": {
   "QueryLanguage": "JSONata",
   "StartAt": "P",
   "States": {
    "P": {
     "Type": "Parallel",
     "Assign": {
      "a": "{% $states.result %}"
     },
     "Branches": [
      {
       "StartAt": "X",
       "States": {
        "X": {
         "Type": "Pass",
         "End": true,
         "Assign": {
          "a": 1
         }
        }
       }
      }
     ],
     "End": true
    }
   }
  },
  "recorded": {
   "status": "SUCCEEDED",
   "output": "[{}]",
   "error": null,
   "cause": null
  }
 }
]"###;

async fn run_to_end(
    client: &aws_sdk_sfn::Client,
    name: &str,
    definition: &str,
) -> Result<aws_sdk_sfn::operation::describe_execution::DescribeExecutionOutput, String> {
    let machine = client
        .create_state_machine()
        .name(name)
        .role_arn(ROLE)
        .definition(definition)
        .send()
        .await
        .map_err(|e| e.message().unwrap_or_default().to_string())?;
    let execution = client
        .start_execution()
        .state_machine_arn(machine.state_machine_arn())
        .input("{}")
        .send()
        .await
        .expect("start execution");
    for _ in 0..200 {
        let described = client
            .describe_execution()
            .execution_arn(execution.execution_arn())
            .send()
            .await
            .expect("describe execution");
        if described.status() != &ExecutionStatus::Running {
            return Ok(described);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{name} did not finish");
}

#[tokio::test]
async fn definitions_behave_as_recorded_from_aws() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let cases: Vec<Value> = serde_json::from_str(RECORDED).unwrap();
    assert!(cases.len() >= 30);

    let mut mismatches = Vec::new();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let recorded = &case["recorded"];
        let definition = case["definition"].to_string();
        let machine = name.replace('_', "-");
        let result = run_to_end(&client, &machine, &definition).await;
        let described = match (result, recorded["create_error"].as_str()) {
            (Err(actual), Some(expected)) => {
                if actual != expected {
                    mismatches.push(format!(
                        "{name}: {actual}
  recorded: {expected}"
                    ));
                }
                continue;
            }
            (Err(actual), None) => {
                mismatches.push(format!("{name}: creation refused: {actual}"));
                continue;
            }
            (Ok(_), Some(expected)) => {
                mismatches.push(format!("{name}: created, recorded: {expected}"));
                continue;
            }
            (Ok(described), None) => described,
        };
        let checks = [
            ("status", Some(described.status().as_str())),
            ("output", described.output()),
            ("error", described.error()),
            ("cause", described.cause()),
        ];
        for (field, actual) in checks {
            if actual != recorded[field].as_str() {
                mismatches.push(format!(
                    "{name}: {field}: {actual:?}
  recorded: {}",
                    recorded[field]
                ));
            }
        }
        if let Some(expected) = recorded["types"].as_array() {
            let history = client
                .get_execution_history()
                .execution_arn(described.execution_arn())
                .max_results(100)
                .send()
                .await
                .expect("history");
            let actual: Vec<&str> = history
                .events()
                .iter()
                .map(|e| e.r#type().as_str())
                .collect();
            let expected: Vec<&str> = expected.iter().filter_map(Value::as_str).collect();
            if actual != expected {
                mismatches.push(format!(
                    "{name}: {actual:?}
  recorded: {expected:?}"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{}",
        mismatches.join(
            "
"
        )
    );
}

#[tokio::test]
async fn evaluation_failures_name_the_nested_field_as_location() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let definition = serde_json::json!({
        "QueryLanguage": "JSONata",
        "StartAt": "A",
        "States": {"A": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sfn:listStateMachines",
            "Arguments": {"maxResults": "{% $states.input.missing %}"},
            "End": true
        }}
    })
    .to_string();
    let described = run_to_end(&client, "location", &definition).await.unwrap();
    let history = client
        .get_execution_history()
        .execution_arn(described.execution_arn())
        .send()
        .await
        .unwrap();
    let raw: Vec<_> = history
        .events()
        .iter()
        .filter(|e| e.r#type().as_str() == "EvaluationFailed")
        .collect();
    let details = raw[0].evaluation_failed_event_details().expect("details");
    assert_eq!(details.location(), Some("Arguments/maxResults"));
}

#[tokio::test]
async fn s3_get_object_fails_as_recorded_on_binary_and_oversized_bodies() {
    let server = TestServer::start().await;
    let client = server.sfn_client().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("recorded").send().await.unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("bin")
        .body((0u8..=255).collect::<Vec<u8>>().into())
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("big")
        .body(vec![b'x'; 300_000].into())
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("recorded")
        .key("fits")
        .body(vec![b'x'; 200_000].into())
        .send()
        .await
        .unwrap();
    let definition = |key: &str| {
        serde_json::json!({"StartAt": "T", "States": {"T": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:s3:getObject",
            "Parameters": {"Bucket": "recorded", "Key": key},
            "End": true
        }}})
        .to_string()
    };

    let binary = run_to_end(&client, "binary", &definition("bin"))
        .await
        .unwrap();
    assert_eq!(binary.error(), Some("S3.InvalidContent"));
    assert_eq!(binary.cause(), Some("Failed to convert 'Body' to string"));

    let big = run_to_end(&client, "big", &definition("big"))
        .await
        .unwrap();
    assert_eq!(big.error(), Some("States.DataLimitExceeded"));
    assert_eq!(
        big.cause(),
        Some(
            "The state/task 'aws-sdk:s3' returned a result with a size exceeding the maximum \
             number of bytes service limit."
        )
    );

    let fits = run_to_end(&client, "fits", &definition("fits"))
        .await
        .unwrap();
    assert_eq!(fits.status(), &ExecutionStatus::Succeeded);
}

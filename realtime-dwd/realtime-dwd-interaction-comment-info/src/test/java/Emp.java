import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author rainbow
 * @time 2025-03-06 14:55
 * @description ...
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
    Integer empNo;

    String eName;

    Integer deptNo;

    Long ts;
}

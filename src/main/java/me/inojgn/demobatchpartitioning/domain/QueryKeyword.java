package me.inojgn.demobatchpartitioning.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@Entity
@NoArgsConstructor
@Table
public class QueryKeyword {

    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;

    @Column
    Long contextNum;

    @Column
    String keyword;

    @Column
    String context;
}

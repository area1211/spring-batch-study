package me.inojgn.demobatchpartitioning.domain.user;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface UserRepository extends JpaRepository<User, Long> {

    User findByEmail(String email);

    @Query("select u from User u where u.idx >= :fromId and u.idx <= :toId")
    List<User> findUsersById(@Param("fromId") Long fromId, @Param("toId") Long toId);
}

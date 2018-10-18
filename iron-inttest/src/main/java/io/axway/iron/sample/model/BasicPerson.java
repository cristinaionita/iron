package io.axway.iron.sample.model;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Unique;

@Entity
public interface BasicPerson {
    @Unique
    String id();

    String name();

    @Nullable
    Double salary();

}

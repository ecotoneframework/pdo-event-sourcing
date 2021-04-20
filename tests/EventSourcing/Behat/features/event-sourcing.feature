Feature: activating as aggregate order entity

  Scenario: I verify building polling projection
    Given I active messaging for namespaces
        | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
        | Test\Ecotone\EventSourcing\Fixture\TicketWithPollingProjection |
    When I register "alert" ticket 123 with assignation to "Johny"
    When I run endpoint with name "inProgressTicketList"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    When I close ticket with id 123
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    And I run endpoint with name "inProgressTicketList"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |

  Scenario: I verify building synchronous event driven projection
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
    When I register "alert" ticket 123 with assignation to "Johny"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    When I close ticket with id 123
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |

  Scenario: I verify building asynchronous event driven projection
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
      | Test\Ecotone\EventSourcing\Fixture\TicketWithAsynchronousEventDrivenProjection |
    When I register "alert" ticket 123 with assignation to "Johny"
    When I run endpoint with name "asynchronous_projections"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    When I close ticket with id 123
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 123        | alert          |
    And I run endpoint with name "asynchronous_projections"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |

  Scenario: Operations on the polling projection
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
      | Test\Ecotone\EventSourcing\Fixture\TicketWithPollingProjection |
    When I register "alert" ticket 1234 with assignation to "Marcus"
    And I run endpoint with name "inProgressTicketList"
    And I stop the projection for in progress tickets
    And I run endpoint with name "inProgressTicketList"
    And I register "alert" ticket 12345 with assignation to "Andrew"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
    When I reset the projection for in progress tickets
    And I run endpoint with name "inProgressTicketList"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
      | 12345      | alert          |
    And I delete projection for all in progress tickets
    And I run endpoint with name "inProgressTicketList"
    Then there should be no in progress ticket list

  Scenario: Operations on the event-driven projection
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
    When I register "alert" ticket 1234 with assignation to "Marcus"
    And I stop the projection for in progress tickets
    And I register "alert" ticket 12345 with assignation to "Andrew"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
    When I reset the projection for in progress tickets
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
      | 12345      | alert          |
    And I delete projection for all in progress tickets
    Then there should be no in progress ticket list

  Scenario: Operations on asynchronous event-driven projection
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Ticket                      |
      | Test\Ecotone\EventSourcing\Fixture\TicketWithAsynchronousEventDrivenProjection |
    When I register "alert" ticket 1234 with assignation to "Marcus"
    And I run endpoint with name "asynchronous_projections"
    And I stop the projection for in progress tickets
    And I register "alert" ticket 12345 with assignation to "Andrew"
    And I run endpoint with name "asynchronous_projections"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
    When I reset the projection for in progress tickets
    And I run endpoint with name "asynchronous_projections"
    Then I should see tickets in progress:
      | ticket_id  | ticket_type    |
      | 1234       | alert          |
      | 12345      | alert          |
    And I delete projection for all in progress tickets
    And I run endpoint with name "asynchronous_projections"
    Then there should be no in progress ticket list

  Scenario: I verify building projection from event sourced aggregate using custom stream name and simple arrays in projections
    Given I active messaging for namespaces
      | Test\Ecotone\EventSourcing\Fixture\Basket                      |
    When I create basket with id 1000
    Then I should see baskets:
      | id    | products    |
      | 1000  | []          |
    When I add product "milk" to basket with id 1000
    Then I should see baskets:
      | id    | products    |
      | 1000  | ["milk"]    |
